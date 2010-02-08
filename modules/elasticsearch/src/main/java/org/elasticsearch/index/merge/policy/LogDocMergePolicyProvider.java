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
import org.apache.lucene.index.LogDocMergePolicy;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.IndexShardLifecycle;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.util.Preconditions;

/**
 * @author kimchy (Shay Banon)
 */
@IndexShardLifecycle
public class LogDocMergePolicyProvider extends AbstractIndexShardComponent implements MergePolicyProvider<LogDocMergePolicy> {

    private final int minMergeDocs;
    private final int maxMergeDocs;
    private final int mergeFactor;
    private final Boolean useCompoundFile;

    @Inject public LogDocMergePolicyProvider(Store store) {
        super(store.shardId(), store.indexSettings());
        Preconditions.checkNotNull(store, "Store must be provided to merge policy");

        this.minMergeDocs = componentSettings.getAsInt("minMergeDocs", LogDocMergePolicy.DEFAULT_MIN_MERGE_DOCS);
        this.maxMergeDocs = componentSettings.getAsInt("maxMergeDocs", LogDocMergePolicy.DEFAULT_MAX_MERGE_DOCS);
        this.mergeFactor = componentSettings.getAsInt("mergeFactor", LogDocMergePolicy.DEFAULT_MERGE_FACTOR);
        this.useCompoundFile = componentSettings.getAsBoolean("useCompoundFile", store == null || store.suggestUseCompoundFile());
        logger.debug("Using [LogDoc] merge policy with mergeFactor[{}] minMergeDocs[{}], maxMergeDocs[{}], useCompoundFile[{}]",
                new Object[]{mergeFactor, minMergeDocs, maxMergeDocs, useCompoundFile});
    }

    @Override public LogDocMergePolicy newMergePolicy(IndexWriter indexWriter) {
        LogDocMergePolicy mergePolicy = new LogDocMergePolicy(indexWriter);
        mergePolicy.setMinMergeDocs(minMergeDocs);
        mergePolicy.setMaxMergeDocs(maxMergeDocs);
        mergePolicy.setMergeFactor(mergeFactor);
        mergePolicy.setUseCompoundFile(useCompoundFile);
        mergePolicy.setUseCompoundDocStore(useCompoundFile);
        return mergePolicy;
    }
}
