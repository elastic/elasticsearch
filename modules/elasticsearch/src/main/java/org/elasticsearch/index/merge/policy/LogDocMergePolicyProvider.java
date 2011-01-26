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
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.store.Store;

import java.io.IOException;
import java.util.Set;

/**
 * @author kimchy (shay.banon)
 */
public class LogDocMergePolicyProvider extends AbstractIndexShardComponent implements MergePolicyProvider<LogDocMergePolicy> {

    private final int minMergeDocs;
    private final int maxMergeDocs;
    private final int mergeFactor;
    private final boolean calibrateSizeByDeletes;
    private boolean asyncMerge;

    @Inject public LogDocMergePolicyProvider(Store store) {
        super(store.shardId(), store.indexSettings());
        Preconditions.checkNotNull(store, "Store must be provided to merge policy");

        this.minMergeDocs = componentSettings.getAsInt("min_merge_docs", LogDocMergePolicy.DEFAULT_MIN_MERGE_DOCS);
        this.maxMergeDocs = componentSettings.getAsInt("max_merge_docs", LogDocMergePolicy.DEFAULT_MAX_MERGE_DOCS);
        this.mergeFactor = componentSettings.getAsInt("merge_factor", LogDocMergePolicy.DEFAULT_MERGE_FACTOR);
        this.calibrateSizeByDeletes = componentSettings.getAsBoolean("calibrate_size_by_deletes", true);
        this.asyncMerge = indexSettings.getAsBoolean("index.merge.async", true);
        logger.debug("using [log_doc] merge policy with merge_factor[{}], min_merge_docs[{}], max_merge_docs[{}], calibrate_size_by_deletes[{}], async_merge[{}]",
                mergeFactor, minMergeDocs, maxMergeDocs, calibrateSizeByDeletes, asyncMerge);
    }

    @Override public LogDocMergePolicy newMergePolicy(IndexWriter indexWriter) {
        LogDocMergePolicy mergePolicy;
        if (asyncMerge) {
            mergePolicy = new EnableMergeLogDocMergePolicy(indexWriter);
        } else {
            mergePolicy = new LogDocMergePolicy(indexWriter);
        }
        mergePolicy.setMinMergeDocs(minMergeDocs);
        mergePolicy.setMaxMergeDocs(maxMergeDocs);
        mergePolicy.setMergeFactor(mergeFactor);
        mergePolicy.setCalibrateSizeByDeletes(calibrateSizeByDeletes);
        return mergePolicy;
    }

    public static class EnableMergeLogDocMergePolicy extends LogDocMergePolicy implements EnableMergePolicy {

        private final ThreadLocal<Boolean> enableMerge = new ThreadLocal<Boolean>() {
            @Override protected Boolean initialValue() {
                return Boolean.FALSE;
            }
        };

        public EnableMergeLogDocMergePolicy(IndexWriter writer) {
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
