// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include "db/filename.h"
#include "db/dbformat.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include <sys/time.h>

#define JC_DEBUG

namespace leveldb {

//zjc 20180507
#ifdef JC_DEBUG
static FILE *log_fp2 = fopen("/tmp/jc_create2.log", "a+");
static char time_buf[30];
static time_t rawtime;
static struct tm * timeinfo;
#endif


Status BuildTable(const std::string& dbname,
                  Env* env,
                  const Options& options,
                  TableCache* table_cache,
                  Iterator* iter,
                  FileMetaData* meta) {
  Status s;
  meta->file_size = 0;
  iter->SeekToFirst();
  std::string fname = TableFileName(dbname, meta->number);



  if (iter->Valid()) {
    WritableFile* file;

  //zjc 20180507
#ifdef JC_DEBUG
  time ( &rawtime );
  timeinfo = localtime ( &rawtime );
  strftime(time_buf, 30, "%x %X", timeinfo);
  fprintf(log_fp2, "[jc_log %s] Builder   Create: %s level-0?\n", time_buf,  fname.c_str());
#endif
  std::string ldb_fmt = "ldb";
  std::string name_fmt = fname.substr(fname.size() - 3);
  if (!(name_fmt == ldb_fmt)) {
      s = env->NewWritableFile(fname, &file);
  } else {
      //std::string level_str = std::to_string(compact->compaction->level());
      std::string fname_used_to_create = fname.substr(0,fname.size()-4) + "START0END.ldb";
      s = env->NewWritableFile(fname_used_to_create, &file);
  }
    //s = env->NewWritableFile(fname, &file);
  //zjc

    if (!s.ok()) {
      return s;
    }

    TableBuilder* builder = new TableBuilder(options, file);
    meta->smallest.DecodeFrom(iter->key());
    for (; iter->Valid(); iter->Next()) {
      Slice key = iter->key();
      meta->largest.DecodeFrom(key);
      builder->Add(key, iter->value());
    }

    // Finish and check for builder errors
    s = builder->Finish();
    if (s.ok()) {
      meta->file_size = builder->FileSize();
      assert(meta->file_size > 0);
    }
    delete builder;

    // Finish and check for file errors
    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }
    delete file;
    file = nullptr;

    if (s.ok()) {
      // Verify that the table is usable
      Iterator* it = table_cache->NewIterator(ReadOptions(),
                                              meta->number,
                                              meta->file_size);
      s = it->status();
      delete it;
    }
  }

  // Check for input iterator errors
  if (!iter->status().ok()) {
    s = iter->status();
  }

  if (s.ok() && meta->file_size > 0) {
    // Keep it
  } else {
    env->DeleteFile(fname);
  }
  return s;
}

}  // namespace leveldb
