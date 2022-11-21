// #pragma once

#include "share/ob_define.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_heap.h"
#include "lib/container/ob_vector.h"
#include "share/io/ob_io_manager.h"
#include "share/scheduler/ob_dag_scheduler.h"
#include "share/ob_thread_pool.h"
#include "blocksstable/ob_block_sstable_struct.h"
#include "blocksstable/ob_tmp_file.h"
#include "share/config/ob_server_config.h"

namespace oceanbase
{
namespace sql
{

template<typename T, typename Compare>
class MemorySortStage : share::ObThreadPool {
public:
  void run1() override
  {
    // init the environment
    ObTenantStatEstGuard stat_est_quard(MTL_ID());
    ObTenantBase *tenant_base = MTL_CTX();
    Worker::CompatMod mode = ((ObTenant *)tenant_base)->get_compat_mode();
    Worker::set_compatibility_mode(mode);
  }
private:
  ObLatch mutex;
};
}

}