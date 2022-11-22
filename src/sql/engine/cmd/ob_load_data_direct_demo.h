#pragma once

#include "lib/file/ob_file.h"
#include "lib/timezone/ob_timezone_info.h"
#include "sql/engine/cmd/ob_load_data_impl.h"
#include "sql/engine/cmd/ob_load_data_parser.h"
#include "storage/blocksstable/ob_index_block_builder.h"
#include "storage/ob_parallel_external_sort.h"
#include "storage/tx_storage/ob_ls_handle.h"

namespace oceanbase
{
namespace sql
{

class ObLoadDataBufferV1
{
public:
  ObLoadDataBufferV1();
  ~ObLoadDataBufferV1();
  // no reuse in ObLoadDataBufferV1
  // void reuse();
  void reset();
  int init(int64_t capacity);
  void set_data(char *data);
  void set_begin(int64_t begin) {
    begin_pos_ = begin;
  }
  void set_capacity(int64_t capacity) {
    capacity_ = capacity;
  }
  OB_INLINE char *data() const { return data_; }
  OB_INLINE char *begin() const { return data_ + begin_pos_; }
  OB_INLINE int64_t begin_pos() const { return begin_pos_; }
  OB_INLINE char *end() const { return data_ + capacity_; }
  OB_INLINE bool empty() const { return capacity_ == begin_pos_; }
  // get capacity
  OB_INLINE int64_t get_capacity() const {
    return capacity_;
  }
  // get remaining data
  OB_INLINE int64_t get_remain_size() const {
    if (NULL == data_) {
      return 0;
    }
    return capacity_ - begin_pos_;
  }
  OB_INLINE void consume(int64_t size) {
    begin_pos_ += size;
  }
private:
  char *data_;
  int64_t begin_pos_; //  now size
  // int64_t end_pos_;
  int64_t capacity_;
};

class ObLoadDataBufferV2
{
public:
  ObLoadDataBufferV2();
  ~ObLoadDataBufferV2();
  // no reuse in ObLoadDataBufferV1
  // void reuse();
  void reset();
  int init(int64_t capacity);
  void set_data(char *data);
  void set_begin(int64_t begin) {
    begin_pos_ = begin;
  }
  void set_real_size(int64_t real_size) {
    real_size_ = real_size;
  }
  OB_INLINE char *data() const { return data_; }
  OB_INLINE char *begin() const { return data_ + begin_pos_; }
  OB_INLINE int64_t begin_pos() const { return begin_pos_; }
  OB_INLINE char *end() const { return data_ + real_size_; }
  OB_INLINE bool empty() const { return real_size_ == begin_pos_; }
  // get capacity
  OB_INLINE int64_t get_capacity() const {
    return capacity_;
  }
  // get real size
  OB_INLINE int64_t get_real_size() const {
    return real_size_;
  }
  // get remaining data
  OB_INLINE int64_t get_remain_size() const {
    if (NULL == data_) {
      return 0;
    }
    return real_size_ - begin_pos_;
  }
  OB_INLINE void consume(int64_t size) {
    begin_pos_ += size;
  }
private:
  char *data_;
  int64_t begin_pos_;
  int64_t capacity_;
  int64_t real_size_;
};


class ObLoadFileReaderV1
{
  static const int OPEN_FLAGS = O_RDONLY;
  // static const int OPEN_MODE;
public:
  ObLoadFileReaderV1();
  ~ObLoadFileReaderV1();
  void reset();
  int open(const ObString &filepath);
  int read_next_buffer(ObLoadDataBufferV1 &buffer);
  int read_next_buffer_v2(ObLoadDataBufferV2 &buffer);
private:
  int fd_;
  int64_t offset_;
  int64_t len_;
  bool is_read_ended_;
};

class ObLoadCSVPaser
{
public:
  ObLoadCSVPaser();
  ~ObLoadCSVPaser();
  void reset();
  int init(const ObDataInFileStruct &format, int64_t column_count,
           common::ObCollationType collation_type);
  int get_next_row(ObLoadDataBufferV1 &buffer, const common::ObNewRow *&row);
  int get_next_row_v2(ObLoadDataBufferV2 &buffer, const common::ObNewRow *&row);
private:
  struct UnusedRowHandler
  {
    int operator()(common::ObIArray<ObCSVGeneralParser::FieldValue> &fields_per_line)
    {
      UNUSED(fields_per_line);
      return OB_SUCCESS;
    }
  };
private:
  common::ObArenaAllocator allocator_;
  common::ObCollationType collation_type_;
  ObCSVGeneralParser csv_parser_;
  common::ObNewRow row_;
  UnusedRowHandler unused_row_handler_;
  common::ObSEArray<ObCSVGeneralParser::LineErrRec, 1> err_records_;
  bool is_inited_;
};

class ObLoadDatumRow
{
  OB_UNIS_VERSION(1);
public:
  ObLoadDatumRow();
  ~ObLoadDatumRow();
  void reset();
  int init(int64_t capacity);
  int64_t get_deep_copy_size() const;
  int deep_copy(const ObLoadDatumRow &src, char *buf, int64_t len, int64_t &pos);
  OB_INLINE bool is_valid() const { return count_ > 0 && nullptr != datums_; }
  DECLARE_TO_STRING;
public:
  common::ObArenaAllocator allocator_;
  int64_t capacity_;
  int64_t count_;
  blocksstable::ObStorageDatum *datums_;
};

class ObLoadDatumRowCompare
{
public:
  ObLoadDatumRowCompare();
  ~ObLoadDatumRowCompare();
  int init(int64_t rowkey_column_num, const blocksstable::ObStorageDatumUtils *datum_utils);
  bool operator()(const ObLoadDatumRow *lhs, const ObLoadDatumRow *rhs);
  int get_error_code() const { return result_code_; }
public:
  int result_code_;
private:
  int64_t rowkey_column_num_;
  const blocksstable::ObStorageDatumUtils *datum_utils_;
  blocksstable::ObDatumRowkey lhs_rowkey_;
  blocksstable::ObDatumRowkey rhs_rowkey_;
  bool is_inited_;
};

class ObLoadRowCaster
{
public:
  ObLoadRowCaster();
  ~ObLoadRowCaster();
  int init(const share::schema::ObTableSchema *table_schema,
           const common::ObIArray<ObLoadDataStmt::FieldOrVarStruct> &field_or_var_list);
  int get_casted_row(const common::ObNewRow &new_row, const ObLoadDatumRow *&datum_row);
private:
  int init_column_schemas_and_idxs(
    const share::schema::ObTableSchema *table_schema,
    const common::ObIArray<ObLoadDataStmt::FieldOrVarStruct> &field_or_var_list);
  int cast_obj_to_datum(const share::schema::ObColumnSchemaV2 *column_schema,
                        const common::ObObj &obj, blocksstable::ObStorageDatum &datum);
private:
  common::ObArray<const share::schema::ObColumnSchemaV2 *> column_schemas_;
  common::ObArray<int64_t> column_idxs_; // Mapping of store columns to source data columns
  int64_t column_count_;
  common::ObCollationType collation_type_;
  ObLoadDatumRow datum_row_;
  common::ObArenaAllocator cast_allocator_;
  common::ObTimeZoneInfo tz_info_;
  bool is_inited_;
};

class ObLoadExternalSort
{
public:
  ObLoadExternalSort();
  ~ObLoadExternalSort();
  int init(const share::schema::ObTableSchema *table_schema, int64_t mem_size,
           int64_t file_buf_size);
  int append_row(const ObLoadDatumRow &datum_row);
  int close();
  int get_next_row(const ObLoadDatumRow *&datum_row);
private:
  common::ObArenaAllocator allocator_;
  blocksstable::ObStorageDatumUtils datum_utils_;
  ObLoadDatumRowCompare compare_;
  storage::ObExternalSortV1<ObLoadDatumRow, ObLoadDatumRowCompare> external_sort_;
  bool is_closed_;
  bool is_inited_;
};

class ObLoadSSTableWriter
{
public:
  ObLoadSSTableWriter();
  ~ObLoadSSTableWriter();
  int init(const share::schema::ObTableSchema *table_schema);
  int append_row(const ObLoadDatumRow &datum_row);
  int close();
private:
  int init_sstable_index_builder(const share::schema::ObTableSchema *table_schema);
  int init_macro_block_writer(const share::schema::ObTableSchema *table_schema);
  int create_sstable();
private:
  common::ObTabletID tablet_id_;
  storage::ObTabletHandle tablet_handle_;
  share::ObLSID ls_id_;
  storage::ObLSHandle ls_handle_;
  int64_t rowkey_column_num_;
  int64_t extra_rowkey_column_num_;
  int64_t column_count_;
  storage::ObITable::TableKey table_key_;
  blocksstable::ObSSTableIndexBuilder sstable_index_builder_;
  blocksstable::ObDataStoreDesc data_store_desc_;
  blocksstable::ObMacroBlockWriter macro_block_writer_;
  blocksstable::ObDatumRow datum_row_;
  bool is_closed_;
  bool is_inited_;
};

class ObLoadDataDirectDemo : public ObLoadDataBase
{
  static const int64_t MEM_BUFFER_SIZE = (1LL << 30); // 1G
  static const int64_t FILE_BUFFER_SIZE = (2LL << 20); // 2M
public:
  ObLoadDataDirectDemo();
  virtual ~ObLoadDataDirectDemo();
  int execute(ObExecContext &ctx, ObLoadDataStmt &load_stmt) override;
private:
  int inner_init(ObLoadDataStmt &load_stmt);
  int do_load();
private:
  ObLoadCSVPaser csv_parser_;
  // ObLoadSequentialFileReader file_reader_;
  // ObLoadDataBuffer buffer_;
  ObLoadFileReaderV1 file_reader_;
  ObLoadDataBufferV1 buffer_;
  ObLoadRowCaster row_caster_;
  ObLoadExternalSort external_sort_;
  ObLoadSSTableWriter sstable_writer_;
};

} // namespace sql
} // namespace oceanbase
