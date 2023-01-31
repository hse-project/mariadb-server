/* Copyright (c) 2023 Micron Technology, Inc.

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; version 2 of the License.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1335  USA */

#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation // gcc: Class implementation
#endif

#define MYSQL_SERVER 1

#include <my_global.h>
#include "sql_priv.h"
#include "unireg.h"
#include "ha_hse.h"
#include "sql_class.h" // THD, SYSTEM_THREAD_SLAVE_SQL

/**
  Checks if the param 'thd' is pointing to slave applier thread and row based
  replication is in use.

  A row event will have its thd->query() == NULL except in cases where
  replicate_annotate_row_events is enabled. In the later case the thd->query()
  will be pointing to the query, received through replicated annotate event
  from master.

  @param thd   pointer to a THD instance

  @return TRUE if thread is slave applier and row based replication is in use
*/
static bool is_row_based_replication(THD *thd)
{
  return thd->system_thread == SYSTEM_THREAD_SLAVE_SQL &&
         (thd->query() == NULL || thd->variables.binlog_annotate_row_events);
}

static handler *hse_create_handler(handlerton *hton, TABLE_SHARE *table,
                                   MEM_ROOT *mem_root)
{
  return new (mem_root) ha_hse(hton, table);
}

static mysql_mutex_t hse_mutex;
static HASH hse_open_tables;

static st_hse_share *get_share(const char *table_name);
static void free_share(st_hse_share *share);

ha_hse::ha_hse(handlerton *hton, TABLE_SHARE *table_arg)
    : handler(hton, table_arg)
{
}

int ha_hse::open(const char *name, int mode, uint test_if_locked)
{
  DBUG_ENTER("ha_hse::open");

  if (!(share= get_share(name)))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);

  thr_lock_data_init(&share->lock, &lock, NULL);
  DBUG_RETURN(0);
}

int ha_hse::close(void)
{
  DBUG_ENTER("ha_hse::close");
  free_share(share);
  DBUG_RETURN(0);
}

int ha_hse::create(const char *name, TABLE *table_arg,
                   HA_CREATE_INFO *create_info)
{
  DBUG_ENTER("ha_hse::create");
  DBUG_RETURN(0);
}

/*
  Intended to support partitioning.
  Allows a particular partition to be truncated.
*/
int ha_hse::truncate()
{
  DBUG_ENTER("ha_hse::truncate");
  DBUG_RETURN(0);
}

const char *ha_hse::index_type(uint key_number)
{
  DBUG_ENTER("ha_hse::index_type");
  DBUG_RETURN(
      (table_share->key_info[key_number].flags & HA_FULLTEXT)  ? "FULLTEXT"
      : (table_share->key_info[key_number].flags & HA_SPATIAL) ? "SPATIAL"
      : (table_share->key_info[key_number].algorithm == HA_KEY_ALG_RTREE)
          ? "RTREE"
          : "BTREE");
}

int ha_hse::write_row(const uchar *buf)
{
  DBUG_ENTER("ha_hse::write_row");
  DBUG_RETURN(table->next_number_field ? update_auto_increment() : 0);
}

int ha_hse::update_row(const uchar *old_data, const uchar *new_data)
{
  DBUG_ENTER("ha_hse::update_row");
  THD *thd= ha_thd();
  if (is_row_based_replication(thd))
    DBUG_RETURN(0);
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

int ha_hse::delete_row(const uchar *buf)
{
  DBUG_ENTER("ha_hse::delete_row");
  THD *thd= ha_thd();
  if (is_row_based_replication(thd))
    DBUG_RETURN(0);
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

int ha_hse::rnd_init(bool scan)
{
  DBUG_ENTER("ha_hse::rnd_init");
  DBUG_RETURN(0);
}

int ha_hse::rnd_next(uchar *buf)
{
  int rc;
  DBUG_ENTER("ha_hse::rnd_next");
  THD *thd= ha_thd();
  if (is_row_based_replication(thd))
    rc= 0;
  else
    rc= HA_ERR_END_OF_FILE;
  DBUG_RETURN(rc);
}

int ha_hse::rnd_pos(uchar *buf, uchar *pos)
{
  DBUG_ENTER("ha_hse::rnd_pos");
  DBUG_ASSERT(0);
  DBUG_RETURN(0);
}

void ha_hse::position(const uchar *record)
{
  DBUG_ENTER("ha_hse::position");
  DBUG_ASSERT(0);
  DBUG_VOID_RETURN;
}

int ha_hse::info(uint flag)
{
  DBUG_ENTER("ha_hse::info");

  bzero((char *) &stats, sizeof(stats));
  if (flag & HA_STATUS_AUTO)
    stats.auto_increment_value= 1;
  DBUG_RETURN(0);
}

int ha_hse::external_lock(THD *thd, int lock_type)
{
  DBUG_ENTER("ha_hse::external_lock");
  DBUG_RETURN(0);
}

THR_LOCK_DATA **ha_hse::store_lock(THD *thd, THR_LOCK_DATA **to,
                                   enum thr_lock_type lock_type)
{
  DBUG_ENTER("ha_hse::store_lock");
  if (lock_type != TL_IGNORE && lock.type == TL_UNLOCK)
  {
    /*
      Here is where we get into the guts of a row level lock.
      If TL_UNLOCK is set
      If we are not doing a LOCK TABLE or DISCARD/IMPORT
      TABLESPACE, then allow multiple writers
    */

    if ((lock_type >= TL_WRITE_CONCURRENT_INSERT && lock_type <= TL_WRITE) &&
        !thd_in_lock_tables(thd) && !thd_tablespace_op(thd))
      lock_type= TL_WRITE_ALLOW_WRITE;

    /*
      In queries of type INSERT INTO t1 SELECT ... FROM t2 ...
      MySQL would use the lock TL_READ_NO_INSERT on t2, and that
      would conflict with TL_WRITE_ALLOW_WRITE, blocking all inserts
      to t2. Convert the lock to a normal read lock to allow
      concurrent inserts to t2.
    */

    if (lock_type == TL_READ_NO_INSERT && !thd_in_lock_tables(thd))
      lock_type= TL_READ;

    lock.type= lock_type;
  }
  *to++= &lock;
  DBUG_RETURN(to);
}

int ha_hse::index_read_map(uchar *buf, const uchar *key,
                           key_part_map keypart_map,
                           enum ha_rkey_function find_flag)
{
  int rc;
  DBUG_ENTER("ha_hse::index_read");
  THD *thd= ha_thd();
  if (is_row_based_replication(thd))
    rc= 0;
  else
    rc= HA_ERR_END_OF_FILE;
  DBUG_RETURN(rc);
}

int ha_hse::index_read_idx_map(uchar *buf, uint idx, const uchar *key,
                               key_part_map keypart_map,
                               enum ha_rkey_function find_flag)
{
  int rc;
  DBUG_ENTER("ha_hse::index_read_idx");
  THD *thd= ha_thd();
  if (is_row_based_replication(thd))
    rc= 0;
  else
    rc= HA_ERR_END_OF_FILE;
  DBUG_RETURN(rc);
}

int ha_hse::index_read_last_map(uchar *buf, const uchar *key,
                                key_part_map keypart_map)
{
  int rc;
  DBUG_ENTER("ha_hse::index_read_last");
  THD *thd= ha_thd();
  if (is_row_based_replication(thd))
    rc= 0;
  else
    rc= HA_ERR_END_OF_FILE;
  DBUG_RETURN(rc);
}

int ha_hse::index_next(uchar *buf)
{
  int rc;
  DBUG_ENTER("ha_hse::index_next");
  rc= HA_ERR_END_OF_FILE;
  DBUG_RETURN(rc);
}

int ha_hse::index_prev(uchar *buf)
{
  int rc;
  DBUG_ENTER("ha_hse::index_prev");
  rc= HA_ERR_END_OF_FILE;
  DBUG_RETURN(rc);
}

int ha_hse::index_first(uchar *buf)
{
  int rc;
  DBUG_ENTER("ha_hse::index_first");
  rc= HA_ERR_END_OF_FILE;
  DBUG_RETURN(rc);
}

int ha_hse::index_last(uchar *buf)
{
  int rc;
  DBUG_ENTER("ha_hse::index_last");
  rc= HA_ERR_END_OF_FILE;
  DBUG_RETURN(rc);
}

static st_hse_share *get_share(const char *table_name)
{
  st_hse_share *share;
  uint length;

  length= (uint) strlen(table_name);
  mysql_mutex_lock(&hse_mutex);

  if (!(share= (st_hse_share *) my_hash_search(&hse_open_tables,
                                               (uchar *) table_name, length)))
  {
    if (!(share= (st_hse_share *) my_malloc(PSI_INSTRUMENT_ME,
                                            sizeof(st_hse_share) + length,
                                            MYF(MY_WME | MY_ZEROFILL))))
      goto error;

    share->table_name_length= length;
    strmov(share->table_name, table_name);

    if (my_hash_insert(&hse_open_tables, (uchar *) share))
    {
      my_free(share);
      share= NULL;
      goto error;
    }

    thr_lock_init(&share->lock);
  }
  share->use_count++;

error:
  mysql_mutex_unlock(&hse_mutex);
  return share;
}

static void free_share(st_hse_share *share)
{
  mysql_mutex_lock(&hse_mutex);
  if (!--share->use_count)
    my_hash_delete(&hse_open_tables, (uchar *) share);
  mysql_mutex_unlock(&hse_mutex);
}

static void hse_free_key(st_hse_share *share)
{
  thr_lock_delete(&share->lock);
  my_free(share);
}

static uchar *hse_get_key(st_hse_share *share, size_t *length,
                          my_bool not_used __attribute__((unused)))
{
  *length= share->table_name_length;
  return (uchar *) share->table_name;
}

#ifdef HAVE_PSI_INTERFACE
static PSI_mutex_key bh_key_mutex_hse;

static PSI_mutex_info all_hse_mutexes[]= {
    {&bh_key_mutex_hse, "hse", PSI_FLAG_GLOBAL}};

void init_hse_psi_keys()
{
  const char *category= "hse";
  int count;

  if (PSI_server == NULL)
    return;

  count= array_elements(all_hse_mutexes);
  PSI_server->register_mutex(category, all_hse_mutexes, count);
}
#endif

static int hse_init(void *p)
{
  handlerton *hse_hton;

#ifdef HAVE_PSI_INTERFACE
  init_hse_psi_keys();
#endif

  hse_hton= (handlerton *) p;
  // TODO: Change db_type to DB_TYPE_AUTOASSIGN. See struct member comment.
  hse_hton->db_type= DB_TYPE_BLACKHOLE_DB;
  hse_hton->create= hse_create_handler;
  hse_hton->drop_table= [](handlerton *, const char *) { return -1; };
  hse_hton->flags= HTON_CAN_RECREATE;

  mysql_mutex_init(bh_key_mutex_hse, &hse_mutex, MY_MUTEX_INIT_FAST);
  (void) my_hash_init(PSI_INSTRUMENT_ME, &hse_open_tables, system_charset_info,
                      32, 0, 0, (my_hash_get_key) hse_get_key,
                      (my_hash_free_key) hse_free_key, 0);

  return 0;
}

static int hse_fini(void *p)
{
  my_hash_free(&hse_open_tables);
  mysql_mutex_destroy(&hse_mutex);

  return 0;
}

struct st_mysql_storage_engine hse_storage_engine= {
    MYSQL_HANDLERTON_INTERFACE_VERSION};

maria_declare_plugin(hse){
    MYSQL_STORAGE_ENGINE_PLUGIN,
    &hse_storage_engine,
    "HSE",
    "Micron Technology, Inc.",
    "HSE storage engine",
    PLUGIN_LICENSE_GPL,
    hse_init,
    hse_fini,
    0x0100 /* 1.0 */,
    NULL,
    NULL,
    "1.0",
    MariaDB_PLUGIN_MATURITY_EXPERIMENTAL} maria_declare_plugin_end;
