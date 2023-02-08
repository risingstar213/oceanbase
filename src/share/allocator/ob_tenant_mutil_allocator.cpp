/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "ob_tenant_mutil_allocator.h"
#include "lib/objectpool/ob_concurrency_objpool.h"
#include "lib/rc/context.h"
#include "observer/omt/ob_multi_tenant.h"
#include "logservice/palf/log_io_task.h"
#include "logservice/palf/fetch_log_engine.h"
#include "logservice/replayservice/ob_replay_status.h"

namespace oceanbase
{
using namespace share;
using namespace palf;
using namespace logservice;
namespace common
{

ObTenantMutilAllocator::ObTenantMutilAllocator(uint64_t tenant_id)
  : tenant_id_(tenant_id), total_limit_(INT64_MAX), pending_replay_mutator_size_(0),
    LOG_IO_FLUSH_LOG_TASK_SIZE(sizeof(palf::LogIOFlushLogTask)),
    LOG_SLIDING_CB_TASK_SIZE(sizeof(palf::LogSlidingCbTask)),
    LOG_IO_TRUNCATE_LOG_TASK_SIZE(sizeof(palf::LogIOTruncateLogTask)),
    LOG_IO_FLUSH_META_TASK_SIZE(sizeof(palf::LogIOFlushMetaTask)),
    LOG_IO_TRUNCATE_PREFIX_BLOCKS_TASK_SIZE(sizeof(palf::LogIOTruncatePrefixBlocksTask)),
    PALF_FETCH_LOG_TASK_SIZE(sizeof(palf::FetchLogTask)),
    LOG_IO_FLASHBACK_TASK_SIZE(sizeof(palf::LogIOFlashbackTask)),
    clog_blk_alloc_(),
    inner_table_replay_blk_alloc_(REPLAY_MEM_LIMIT_THRESHOLD * INNER_TABLE_REPLAY_MEM_PERCENT / 100),
    user_table_replay_blk_alloc_(REPLAY_MEM_LIMIT_THRESHOLD * (100 - INNER_TABLE_REPLAY_MEM_PERCENT) / 100),
    common_blk_alloc_(),
    unlimited_blk_alloc_(),
    clog_ge_alloc_(ObMemAttr(tenant_id, ObModIds::OB_CLOG_GE), ObVSliceAlloc::DEFAULT_BLOCK_SIZE, clog_blk_alloc_),
    inner_table_replay_task_alloc_(ObMemAttr(tenant_id, ObModIds::OB_LOG_REPLAY_ENGINE), ObVSliceAlloc::DEFAULT_BLOCK_SIZE, inner_table_replay_blk_alloc_),
    user_table_replay_task_alloc_(ObMemAttr(tenant_id, ObModIds::OB_LOG_REPLAY_ENGINE), ObVSliceAlloc::DEFAULT_BLOCK_SIZE, user_table_replay_blk_alloc_),
    log_io_flush_log_task_alloc_(LOG_IO_FLUSH_LOG_TASK_SIZE, ObMemAttr(tenant_id, "FlushLog"), choose_blk_size(LOG_IO_FLUSH_LOG_TASK_SIZE), clog_blk_alloc_, this),
    log_sliding_cb_task_alloc_(LOG_SLIDING_CB_TASK_SIZE, ObMemAttr(tenant_id, "SlidingCb"), choose_blk_size(LOG_SLIDING_CB_TASK_SIZE), clog_blk_alloc_, this),
    log_io_truncate_log_task_alloc_(LOG_IO_TRUNCATE_LOG_TASK_SIZE, ObMemAttr(tenant_id, "TruncateLog"), choose_blk_size(LOG_IO_TRUNCATE_LOG_TASK_SIZE), clog_blk_alloc_, this),
    log_io_flush_meta_task_alloc_(LOG_IO_FLUSH_META_TASK_SIZE, ObMemAttr(tenant_id, "FlushMeta"), choose_blk_size(LOG_IO_FLUSH_META_TASK_SIZE), clog_blk_alloc_, this),
    log_io_truncate_prefix_blocks_task_alloc_(LOG_IO_TRUNCATE_PREFIX_BLOCKS_TASK_SIZE, ObMemAttr(tenant_id, "FlushMeta"), choose_blk_size(LOG_IO_TRUNCATE_PREFIX_BLOCKS_TASK_SIZE), clog_blk_alloc_, this),
    palf_fetch_log_task_alloc_(PALF_FETCH_LOG_TASK_SIZE, ObMemAttr(tenant_id, ObModIds::OB_FETCH_LOG_TASK), choose_blk_size(PALF_FETCH_LOG_TASK_SIZE), clog_blk_alloc_, this),
    replay_log_task_alloc_(ObMemAttr(tenant_id, ObModIds::OB_LOG_REPLAY_TASK), common::OB_MALLOC_BIG_BLOCK_SIZE),
    log_io_flashback_task_alloc_(LOG_IO_FLASHBACK_TASK_SIZE, ObMemAttr(tenant_id, "Flashback"), choose_blk_size(LOG_IO_FLASHBACK_TASK_SIZE), clog_blk_alloc_, this)
{
  // set_nway according to tenant's max_cpu
  double min_cpu = 0;
  double max_cpu = 0;
  omt::ObMultiTenant *omt = GCTX.omt_;
  if (NULL == omt) {
  } else if (OB_SUCCESS != omt->get_tenant_cpu(tenant_id, min_cpu, max_cpu)) {
  } else {
    const int32_t nway = (int32_t)max_cpu;
    set_nway(nway);
  }
}

int ObTenantMutilAllocator::choose_blk_size(int obj_size)
{
  static const int MIN_SLICE_CNT = 64;
  int blk_size = OB_MALLOC_NORMAL_BLOCK_SIZE;  // default blk size is 8KB
  if (obj_size <= 0) {
  } else if (MIN_SLICE_CNT <= (OB_MALLOC_NORMAL_BLOCK_SIZE / obj_size)) {
  } else if (MIN_SLICE_CNT <= (OB_MALLOC_MIDDLE_BLOCK_SIZE / obj_size)) {
    blk_size = OB_MALLOC_MIDDLE_BLOCK_SIZE;
  } else {
    blk_size = OB_MALLOC_BIG_BLOCK_SIZE;
  }
  return blk_size;
}

void ObTenantMutilAllocator::try_purge()
{
  clog_ge_alloc_.purge_extra_cached_block(0);
  inner_table_replay_task_alloc_.purge_extra_cached_block(0);
  user_table_replay_task_alloc_.purge_extra_cached_block(0);
  log_io_flush_log_task_alloc_.purge_extra_cached_block(0);
  log_sliding_cb_task_alloc_.purge_extra_cached_block(0);
  log_io_truncate_log_task_alloc_.purge_extra_cached_block(0);
  log_io_flush_meta_task_alloc_.purge_extra_cached_block(0);
  log_io_truncate_prefix_blocks_task_alloc_.purge_extra_cached_block(0);
  log_io_flashback_task_alloc_.purge_extra_cached_block(0);
  palf_fetch_log_task_alloc_.purge_extra_cached_block(0);
  replay_log_task_alloc_.purge_extra_cached_block(0);
}

void *ObTenantMutilAllocator::ge_alloc(const int64_t size)
{
  void *ptr = NULL;
  ptr = clog_ge_alloc_.alloc(size);
  return ptr;
}

void ObTenantMutilAllocator::ge_free(void *ptr)
{
  clog_ge_alloc_.free(ptr);
}

void *ObTenantMutilAllocator::alloc(const int64_t size)
{
  return ob_malloc(size, lib::ObMemAttr(tenant_id_, "LogAlloc"));
}

void *ObTenantMutilAllocator::alloc(const int64_t size, const lib::ObMemAttr &attr)
{
  return ob_malloc(size, attr);
}

void ObTenantMutilAllocator::free(void *ptr)
{
  ob_free(ptr);
}

const ObBlockAllocMgr &ObTenantMutilAllocator::get_clog_blk_alloc_mgr() const
{
  return clog_blk_alloc_;
}

void *ObTenantMutilAllocator::alloc_replay_task_buf(const bool is_inner_table, const int64_t size)
{
  void *ptr = NULL;
  ObVSliceAlloc &allocator = is_inner_table ? inner_table_replay_task_alloc_ : user_table_replay_task_alloc_;
  ptr = allocator.alloc(size);
  return ptr;
}

void ObTenantMutilAllocator::free_replay_task(const bool is_inner_table, void *ptr)
{
  if (OB_LIKELY(NULL != ptr)) {
    ObVSliceAlloc &allocator = is_inner_table ? inner_table_replay_task_alloc_ : user_table_replay_task_alloc_;
    allocator.free(ptr);
  }
}

bool ObTenantMutilAllocator::can_alloc_replay_task(const bool is_inner_table, int64_t size) const
{
  const ObVSliceAlloc &allocator = is_inner_table ? inner_table_replay_task_alloc_ : user_table_replay_task_alloc_;
  return allocator.can_alloc_block(size);
}

void ObTenantMutilAllocator::inc_pending_replay_mutator_size(int64_t size)
{
  ATOMIC_AAF(&pending_replay_mutator_size_, size);
}

void ObTenantMutilAllocator::dec_pending_replay_mutator_size(int64_t size)
{
  ATOMIC_SAF(&pending_replay_mutator_size_, size);
}

int64_t ObTenantMutilAllocator::get_pending_replay_mutator_size() const
{
  return ATOMIC_LOAD(&pending_replay_mutator_size_);
}

LogIOFlushLogTask *ObTenantMutilAllocator::alloc_log_io_flush_log_task(
		const int64_t palf_id, const int64_t palf_epoch)
{
  LogIOFlushLogTask *ret_ptr = NULL;
  void *ptr = log_io_flush_log_task_alloc_.alloc();
  if (NULL != ptr) {
    ret_ptr = new(ptr)LogIOFlushLogTask(palf_id, palf_epoch);
    ATOMIC_INC(&flying_log_task_);
  }
  return ret_ptr;
}

void ObTenantMutilAllocator::free_log_io_flush_log_task(LogIOFlushLogTask *ptr)
{
  if (OB_LIKELY(NULL != ptr)) {
    ptr->~LogIOFlushLogTask();
    log_io_flush_log_task_alloc_.free(ptr);
    ATOMIC_DEC(&flying_log_task_);
  }
}

LogSlidingCbTask *ObTenantMutilAllocator::alloc_log_sliding_cb_task(
		const int64_t palf_id, const int64_t palf_epoch)
{
  LogSlidingCbTask *ret_ptr = NULL;
  void *ptr = log_sliding_cb_task_alloc_.alloc();
  if (NULL != ptr) {
    ret_ptr = new(ptr)LogSlidingCbTask(palf_id, palf_epoch);
    ATOMIC_INC(&flying_sliding_cb_task_);
  }
  return ret_ptr;
}

void ObTenantMutilAllocator::free_log_sliding_cb_task(LogSlidingCbTask *ptr)
{
  if (OB_LIKELY(NULL != ptr)) {
    ptr->~LogSlidingCbTask();
    log_sliding_cb_task_alloc_.free(ptr);
    ATOMIC_DEC(&flying_sliding_cb_task_);
  }
}


LogIOTruncateLogTask *ObTenantMutilAllocator::alloc_log_io_truncate_log_task(
		const int64_t palf_id, const int64_t palf_epoch)
{
  LogIOTruncateLogTask *ret_ptr = NULL;
  void *ptr = log_io_truncate_log_task_alloc_.alloc();
  if (NULL != ptr) {
    ret_ptr = new(ptr) LogIOTruncateLogTask(palf_id, palf_epoch);
  }
  return ret_ptr;
}

void ObTenantMutilAllocator::free_log_io_truncate_log_task(LogIOTruncateLogTask *ptr)
{
  if (OB_LIKELY(NULL != ptr)) {
    ptr->~LogIOTruncateLogTask();
    log_io_truncate_log_task_alloc_.free(ptr);
  }
}

LogIOFlushMetaTask *ObTenantMutilAllocator::alloc_log_io_flush_meta_task(
		const int64_t palf_id, const int64_t palf_epoch)
{
  LogIOFlushMetaTask *ret_ptr = NULL;
  void *ptr = log_io_flush_meta_task_alloc_.alloc();
  if (NULL != ptr) {
    ret_ptr = new(ptr)LogIOFlushMetaTask(palf_id, palf_epoch);
    ATOMIC_INC(&flying_meta_task_);
  }
  return ret_ptr;
}

void ObTenantMutilAllocator::free_log_io_flush_meta_task(LogIOFlushMetaTask *ptr)
{
  if (OB_LIKELY(NULL != ptr)) {
    ptr->~LogIOFlushMetaTask();
    log_io_flush_meta_task_alloc_.free(ptr);
    ATOMIC_DEC(&flying_meta_task_);
  }
}

palf::LogIOTruncatePrefixBlocksTask *ObTenantMutilAllocator::alloc_log_io_truncate_prefix_blocks_task(
		const int64_t palf_id, const int64_t palf_epoch)
{
  LogIOTruncatePrefixBlocksTask *ret_ptr = NULL;
  void *ptr = log_io_truncate_prefix_blocks_task_alloc_.alloc();
  if (NULL != ptr) {
    ret_ptr = new(ptr)LogIOTruncatePrefixBlocksTask(palf_id ,palf_epoch);
  }
  return ret_ptr;
}

void ObTenantMutilAllocator::free_log_io_truncate_prefix_blocks_task(palf::LogIOTruncatePrefixBlocksTask *ptr)
{
  if (OB_LIKELY(NULL != ptr)) {
    ptr->~LogIOTruncatePrefixBlocksTask();
    log_io_truncate_prefix_blocks_task_alloc_.free(ptr);
  }
}

palf::FetchLogTask *ObTenantMutilAllocator::alloc_palf_fetch_log_task()
{
  FetchLogTask *ret_ptr = NULL;
  void *ptr = palf_fetch_log_task_alloc_.alloc();
  if (NULL != ptr) {
    ret_ptr = new(ptr)FetchLogTask();
  }
  return ret_ptr;
}

void ObTenantMutilAllocator::free_palf_fetch_log_task(palf::FetchLogTask *ptr)
{
  if (OB_LIKELY(NULL != ptr)) {
    ptr->~FetchLogTask();
    palf_fetch_log_task_alloc_.free(ptr);
  }
}

void *ObTenantMutilAllocator::alloc_replay_task(const int64_t size)
{
  return replay_log_task_alloc_.alloc(size);
}

void *ObTenantMutilAllocator::alloc_replay_log_buf(const int64_t size)
{
  return replay_log_task_alloc_.alloc(size);
}

void ObTenantMutilAllocator::free_replay_task(logservice::ObLogReplayTask *ptr)
{
  if (OB_LIKELY(NULL != ptr)) {
    ptr->~ObLogReplayTask();
    replay_log_task_alloc_.free(ptr);
  }
}

void ObTenantMutilAllocator::free_replay_log_buf(void *ptr)
{
  if (OB_LIKELY(NULL != ptr)) {
    replay_log_task_alloc_.free(ptr);
  }
}

palf::LogIOFlashbackTask *ObTenantMutilAllocator::alloc_log_io_flashback_task(const int64_t palf_id, const int64_t palf_epoch)
{
  LogIOFlashbackTask *ret_ptr = NULL;
  void *ptr = log_io_flashback_task_alloc_.alloc();
  if (NULL != ptr) {
    ret_ptr = new(ptr)LogIOFlashbackTask(palf_id, palf_epoch);
  }
  return ret_ptr;
}

void ObTenantMutilAllocator::free_log_io_flashback_task(palf::LogIOFlashbackTask *ptr)
{
  if (OB_LIKELY(NULL != ptr)) {
    ptr->~LogIOFlashbackTask();
    log_io_flashback_task_alloc_.free(ptr);
  }
}

void ObTenantMutilAllocator::set_nway(const int32_t nway)
{
  if (nway > 0) {
    clog_ge_alloc_.set_nway(nway);
    inner_table_replay_task_alloc_.set_nway(nway);
    user_table_replay_task_alloc_.set_nway(nway);
    OB_LOG(INFO, "finish set nway", K(tenant_id_), K(nway));
  }
}

void ObTenantMutilAllocator::set_limit(const int64_t total_limit)
{
  if (total_limit > 0 && total_limit != ATOMIC_LOAD(&total_limit_)) {
    ATOMIC_STORE(&total_limit_, total_limit);
    const int64_t clog_limit = total_limit / 100 * CLOG_MEM_LIMIT_PERCENT;
    const int64_t replay_limit = std::min(total_limit / 100 * REPLAY_MEM_LIMIT_PERCENT, REPLAY_MEM_LIMIT_THRESHOLD);
    const int64_t inner_table_replay_limit = replay_limit * INNER_TABLE_REPLAY_MEM_PERCENT / 100;
    const int64_t user_table_replay_limit = replay_limit * (100 - INNER_TABLE_REPLAY_MEM_PERCENT) / 100;
    const int64_t common_limit = total_limit - (clog_limit + replay_limit);
    clog_blk_alloc_.set_limit(clog_limit);
    inner_table_replay_blk_alloc_.set_limit(inner_table_replay_limit);
    user_table_replay_blk_alloc_.set_limit(user_table_replay_limit);
    common_blk_alloc_.set_limit(common_limit);
    OB_LOG(INFO, "ObTenantMutilAllocator set tenant mem limit finished", K(tenant_id_), K(total_limit), K(clog_limit),
        K(replay_limit), K(common_limit), K(inner_table_replay_limit), K(user_table_replay_limit));
  }
}

int64_t ObTenantMutilAllocator::get_limit() const
{
  return ATOMIC_LOAD(&total_limit_);
}

int64_t ObTenantMutilAllocator::get_hold() const
{
  return clog_blk_alloc_.hold() + inner_table_replay_blk_alloc_.hold()
      + user_table_replay_blk_alloc_.hold() + common_blk_alloc_.hold();
}

#define SLICE_FREE_OBJ(name, cls) \
void ob_slice_free_##name(typeof(cls) *ptr) \
  { \
    if (NULL != ptr) { \
      ObBlockSlicer::Item *item = (ObBlockSlicer::Item*)ptr - 1; \
      if (NULL != item->host_) { \
        ObTenantMutilAllocator *tma = reinterpret_cast<ObTenantMutilAllocator*>(item->host_->get_tmallocator()); \
        if (NULL != tma) { \
          tma->free_##name(ptr); \
        } \
      } \
    } \
  } \

}
}
