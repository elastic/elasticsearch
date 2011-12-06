/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.index.shard;

import org.elasticsearch.common.component.CloseableComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.jmx.JmxService;
import org.elasticsearch.jmx.MBean;
import org.elasticsearch.jmx.ManagedAttribute;

import java.io.IOException;

import static org.elasticsearch.index.IndexServiceManagement.buildIndexGroupName;

/**
 *
 */
@MBean(objectName = "", description = "")
public class IndexShardManagement extends AbstractIndexShardComponent implements CloseableComponent {

    public static String buildShardGroupName(ShardId shardId) {
        return buildIndexGroupName(shardId.index()) + ",subService=shards,shard=" + shardId.id();
    }

    private final JmxService jmxService;

    private final IndexShard indexShard;

    private final Store store;

    private final Translog translog;

    @Inject
    public IndexShardManagement(ShardId shardId, @IndexSettings Settings indexSettings, JmxService jmxService, IndexShard indexShard,
                                Store store, Translog translog) {
        super(shardId, indexSettings);
        this.jmxService = jmxService;
        this.indexShard = indexShard;
        this.store = store;
        this.translog = translog;
    }

    public void close() {
        jmxService.unregisterGroup(buildShardGroupName(indexShard.shardId()));
    }

    @ManagedAttribute(description = "Index Name")
    public String getIndex() {
        return indexShard.shardId().index().name();
    }

    @ManagedAttribute(description = "Shard Id")
    public int getShardId() {
        return indexShard.shardId().id();
    }

    @ManagedAttribute(description = "Storage Size")
    public String getStoreSize() {
        try {
            return store.estimateSize().toString();
        } catch (IOException e) {
            return "NA";
        }
    }

    @ManagedAttribute(description = "The current transaction log id")
    public long getTranslogId() {
        return translog.currentId();
    }

    @ManagedAttribute(description = "Number of transaction log operations")
    public long getTranslogNumberOfOperations() {
        return translog.estimatedNumberOfOperations();
    }

    @ManagedAttribute(description = "Estimated size in memory the transaction log takes")
    public String getTranslogSize() {
        return new ByteSizeValue(translog.memorySizeInBytes()).toString();
    }

    @ManagedAttribute(description = "The state of the shard")
    public String getState() {
        return indexShard.state().toString();
    }

    @ManagedAttribute(description = "Primary")
    public boolean isPrimary() {
        return indexShard.routingEntry().primary();
    }

    @ManagedAttribute(description = "The state of the shard as perceived by the cluster")
    public String getRoutingState() {
        return indexShard.routingEntry().state().toString();
    }

    @ManagedAttribute(description = "The number of documents in the index")
    public int getNumDocs() {
        Engine.Searcher searcher = indexShard.searcher();
        try {
            return searcher.reader().numDocs();
        } finally {
            searcher.release();
        }
    }

    @ManagedAttribute(description = "The total number of documents in the index (including deleted ones)")
    public int getMaxDoc() {
        Engine.Searcher searcher = indexShard.searcher();
        try {
            return searcher.reader().maxDoc();
        } finally {
            searcher.release();
        }
    }
}
