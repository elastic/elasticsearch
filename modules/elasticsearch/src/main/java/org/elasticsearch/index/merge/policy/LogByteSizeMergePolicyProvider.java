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

import org.apache.lucene.index.*;
import org.elasticsearch.common.Preconditions;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.store.Store;

import java.io.IOException;
import java.util.Set;

/**
 * @author kimchy (shay.banon)
 */
public class LogByteSizeMergePolicyProvider extends AbstractIndexShardComponent implements MergePolicyProvider<LogByteSizeMergePolicy> {

    private final ByteSizeValue minMergeSize;
    private final ByteSizeValue maxMergeSize;
    private final int mergeFactor;
    private final int maxMergeDocs;
    private final boolean calibrateSizeByDeletes;
    private boolean asyncMerge;

    @Inject public LogByteSizeMergePolicyProvider(Store store) {
        super(store.shardId(), store.indexSettings());
        Preconditions.checkNotNull(store, "Store must be provided to merge policy");

        this.minMergeSize = componentSettings.getAsBytesSize("min_merge_size", new ByteSizeValue((long) (LogByteSizeMergePolicy.DEFAULT_MIN_MERGE_MB * 1024 * 1024), ByteSizeUnit.BYTES));
        this.maxMergeSize = componentSettings.getAsBytesSize("max_merge_size", new ByteSizeValue((long) LogByteSizeMergePolicy.DEFAULT_MAX_MERGE_MB, ByteSizeUnit.MB));
        this.mergeFactor = componentSettings.getAsInt("merge_factor", LogByteSizeMergePolicy.DEFAULT_MERGE_FACTOR);
        this.maxMergeDocs = componentSettings.getAsInt("max_merge_docs", LogByteSizeMergePolicy.DEFAULT_MAX_MERGE_DOCS);
        this.calibrateSizeByDeletes = componentSettings.getAsBoolean("calibrate_size_by_deletes", true);
        this.asyncMerge = indexSettings.getAsBoolean("index.merge.async", true);
        logger.debug("using [log_bytes_size] merge policy with merge_factor[{}], min_merge_size[{}], max_merge_size[{}], max_merge_docs[{}], calibrate_size_by_deletes[{}], async_merge[{}]",
                mergeFactor, minMergeSize, maxMergeSize, maxMergeDocs, calibrateSizeByDeletes, asyncMerge);
    }

    @Override public LogByteSizeMergePolicy newMergePolicy(IndexWriter indexWriter) {
        LogByteSizeMergePolicy mergePolicy;
        if (asyncMerge) {
            mergePolicy = new EnableMergeLogByteSizeMergePolicy(indexWriter);
        } else {
            mergePolicy = new LogByteSizeMergePolicy(indexWriter);
        }
        mergePolicy.setMinMergeMB(minMergeSize.mbFrac());
        mergePolicy.setMaxMergeMB(maxMergeSize.mbFrac());
        mergePolicy.setMergeFactor(mergeFactor);
        mergePolicy.setMaxMergeDocs(maxMergeDocs);
        mergePolicy.setCalibrateSizeByDeletes(calibrateSizeByDeletes);
        return mergePolicy;
    }

    public static class EnableMergeLogByteSizeMergePolicy extends LogByteSizeMergePolicy implements EnableMergePolicy {

        private final ThreadLocal<Boolean> enableMerge = new ThreadLocal<Boolean>() {
            @Override protected Boolean initialValue() {
                return Boolean.FALSE;
            }
        };

        public EnableMergeLogByteSizeMergePolicy(IndexWriter writer) {
            super(writer);
        }

        @Override public void enableMerge() {
            enableMerge.set(Boolean.TRUE);
        }

        @Override public void disableMerge() {
            enableMerge.set(Boolean.FALSE);
        }

        @Override public boolean isMergeEnabled() {
            return enableMerge.get() == Boolean.TRUE;
        }

        @Override public void close() {
            enableMerge.remove();
            super.close();
        }

        @Override public MergeSpecification findMerges(SegmentInfos infos) throws IOException {
            if (enableMerge.get() == Boolean.FALSE) {
                return null;
            }
            return super.findMerges(infos);
        }

        @Override public MergeSpecification findMergesToExpungeDeletes(SegmentInfos segmentInfos) throws CorruptIndexException, IOException {
            if (enableMerge.get() == Boolean.FALSE) {
                return null;
            }
            return super.findMergesToExpungeDeletes(segmentInfos);
        }

        @Override public MergeSpecification findMergesForOptimize(SegmentInfos infos, int maxNumSegments, Set<SegmentInfo> segmentsToOptimize) throws IOException {
            if (enableMerge.get() == Boolean.FALSE) {
                return null;
            }
            return super.findMergesForOptimize(infos, maxNumSegments, segmentsToOptimize);
        }
    }
}
