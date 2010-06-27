/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.merge.policy;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.elasticsearch.common.Preconditions;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.store.Store;

/**
 * @author kimchy (shay.banon)
 */
public class LogByteSizeMergePolicyProvider extends AbstractIndexShardComponent implements MergePolicyProvider<LogByteSizeMergePolicy> {

    private final ByteSizeValue minMergeSize;
    private final ByteSizeValue maxMergeSize;
    private final int mergeFactor;
    private final int maxMergeDocs;
    private final boolean calibrateSizeByDeletes;
    private final Boolean useCompoundFile;

    @Inject public LogByteSizeMergePolicyProvider(Store store) {
        super(store.shardId(), store.indexSettings());
        Preconditions.checkNotNull(store, "Store must be provided to merge policy");

        this.minMergeSize = componentSettings.getAsBytesSize("min_merge_size", new ByteSizeValue((long) (LogByteSizeMergePolicy.DEFAULT_MIN_MERGE_MB * 1024 * 1024), ByteSizeUnit.BYTES));
        this.maxMergeSize = componentSettings.getAsBytesSize("max_merge_size", new ByteSizeValue((long) LogByteSizeMergePolicy.DEFAULT_MAX_MERGE_MB, ByteSizeUnit.MB));
        this.mergeFactor = componentSettings.getAsInt("merge_factor", LogByteSizeMergePolicy.DEFAULT_MERGE_FACTOR);
        this.maxMergeDocs = componentSettings.getAsInt("max_merge_docs", LogByteSizeMergePolicy.DEFAULT_MAX_MERGE_DOCS);
        this.calibrateSizeByDeletes = componentSettings.getAsBoolean("calibrate_size_by_deletes", false);
        this.useCompoundFile = componentSettings.getAsBoolean("use_compound_file", store == null || store.suggestUseCompoundFile());
        logger.debug("Using [log_bytes_size] merge policy with merge_factor[{}], min_merge_size[{}], max_merge_size[{}], max_merge_docs[{}] use_compound_file[{}], calibrate_size_by_deletes[{}]",
                mergeFactor, minMergeSize, maxMergeSize, maxMergeDocs, useCompoundFile, calibrateSizeByDeletes);
    }

    @Override public LogByteSizeMergePolicy newMergePolicy(IndexWriter indexWriter) {
        LogByteSizeMergePolicy mergePolicy = new LogByteSizeMergePolicy(indexWriter);
        mergePolicy.setMinMergeMB(minMergeSize.mbFrac());
        mergePolicy.setMaxMergeMB(maxMergeSize.mbFrac());
        mergePolicy.setMergeFactor(mergeFactor);
        mergePolicy.setMaxMergeDocs(maxMergeDocs);
        mergePolicy.setUseCompoundFile(useCompoundFile);
        mergePolicy.setUseCompoundDocStore(useCompoundFile);
        mergePolicy.setCalibrateSizeByDeletes(calibrateSizeByDeletes);
        return mergePolicy;
    }
}
