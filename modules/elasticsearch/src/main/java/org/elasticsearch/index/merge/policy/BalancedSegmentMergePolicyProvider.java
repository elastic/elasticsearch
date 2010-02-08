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

import com.google.inject.Inject;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.LogMergePolicy;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.IndexShardLifecycle;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.util.Preconditions;
import org.elasticsearch.util.SizeUnit;
import org.elasticsearch.util.SizeValue;

/**
 * @author kimchy (Shay Banon)
 */
@IndexShardLifecycle
public class BalancedSegmentMergePolicyProvider extends AbstractIndexShardComponent implements MergePolicyProvider<LogByteSizeMergePolicy> {

    private final SizeValue minMergeSize;
    private final SizeValue maxMergeSize;
    private final int mergeFactor;
    private final int maxMergeDocs;
    private final int numLargeSegments;
    private final int maxSmallSegments;
    private final Boolean useCompoundFile;

    @Inject public BalancedSegmentMergePolicyProvider(Store store) {
        super(store.shardId(), store.indexSettings());
        Preconditions.checkNotNull(store, "Store must be provided to merge policy");

        this.minMergeSize = componentSettings.getAsSize("minMergeSize", new SizeValue((long) LogByteSizeMergePolicy.DEFAULT_MIN_MERGE_MB * 1024 * 1024, SizeUnit.BYTES));
        this.maxMergeSize = componentSettings.getAsSize("maxMergeSize", new SizeValue((long) LogByteSizeMergePolicy.DEFAULT_MAX_MERGE_MB, SizeUnit.MB));
        this.mergeFactor = componentSettings.getAsInt("mergeFactor", LogByteSizeMergePolicy.DEFAULT_MERGE_FACTOR);
        this.maxMergeDocs = componentSettings.getAsInt("maxMergeDocs", LogByteSizeMergePolicy.DEFAULT_MAX_MERGE_DOCS);
        this.numLargeSegments = componentSettings.getAsInt("numLargeSegments", BalancedSegmentMergePolicy.DEFAULT_NUM_LARGE_SEGMENTS);
        this.maxSmallSegments = componentSettings.getAsInt("maxSmallSegments", 2 * LogMergePolicy.DEFAULT_MERGE_FACTOR);

        this.useCompoundFile = componentSettings.getAsBoolean("useCompoundFile", store == null || store.suggestUseCompoundFile());

        logger.debug("Using [Balanced] merge policy with mergeFactor[{}], minMergeSize[{}], maxMergeSize[{}], maxMergeDocs[{}] useCompoundFile[{}]",
                new Object[]{mergeFactor, minMergeSize, maxMergeSize, maxMergeDocs, useCompoundFile});
    }

    @Override public BalancedSegmentMergePolicy newMergePolicy(IndexWriter indexWriter) {
        BalancedSegmentMergePolicy mergePolicy = new BalancedSegmentMergePolicy(indexWriter);
        mergePolicy.setMinMergeMB(minMergeSize.mbFrac());
        mergePolicy.setMaxMergeMB(maxMergeSize.mbFrac());
        mergePolicy.setMergeFactor(mergeFactor);
        mergePolicy.setMaxMergeDocs(maxMergeDocs);
        mergePolicy.setUseCompoundFile(useCompoundFile);
        mergePolicy.setUseCompoundDocStore(useCompoundFile);

        mergePolicy.setMaxSmallSegments(maxSmallSegments);
        mergePolicy.setNumLargeSegments(numLargeSegments);
        return mergePolicy;
    }
}