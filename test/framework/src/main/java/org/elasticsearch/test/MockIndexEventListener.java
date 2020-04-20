/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
package org.elasticsearch.test;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

/**
 * This is a testing plugin that registers a generic
 * {@link MockIndexEventListener.TestEventListener} as a node level service
 * as well as a listener on every index. Tests can access it like this:
 * <pre>
 *     TestEventListener listener = internalCluster().getInstance(MockIndexEventListener.TestEventListener.class, node1);
 *     listener.setNewDelegate(new IndexEventListener() {
 *        // do some stuff
 *     });
 * </pre>
 * This allows tests to use the listener without registering their own plugins.
 */
public final class MockIndexEventListener {

    public static class TestPlugin extends Plugin {
        private final TestEventListener listener = new TestEventListener();

        /**
         * For tests to pass in to fail on listener invocation
         */
        public static final Setting<Boolean> INDEX_FAIL = Setting.boolSetting("index.fail", false, Property.IndexScope);
        @Override
        public List<Setting<?>> getSettings() {
            return Arrays.asList(INDEX_FAIL);
        }

        @Override
        public void onIndexModule(IndexModule module) {
            module.addIndexEventListener(listener);
        }

        @Override
        public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
                                                   ResourceWatcherService resourceWatcherService, ScriptService scriptService,
                                                   NamedXContentRegistry xContentRegistry, Environment environment,
                                                   NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry,
                                                   IndexNameExpressionResolver expressionResolver,
                                                   Supplier<RepositoriesService> repositoriesServiceSupplier) {
            return Collections.singletonList(listener);
        }
    }

    public static class TestEventListener implements IndexEventListener {
        private volatile IndexEventListener delegate = new IndexEventListener() {};

        public void setNewDelegate(IndexEventListener listener) {
            delegate = listener == null ? new IndexEventListener() {} : listener;
        }

        @Override
        public void shardRoutingChanged(IndexShard indexShard, @Nullable ShardRouting oldRouting, ShardRouting newRouting) {
            delegate.shardRoutingChanged(indexShard, oldRouting, newRouting);
        }

        @Override
        public void afterIndexShardCreated(IndexShard indexShard) {
            delegate.afterIndexShardCreated(indexShard);
        }

        @Override
        public void afterIndexShardStarted(IndexShard indexShard) {
            delegate.afterIndexShardStarted(indexShard);
        }

        @Override
        public void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
            delegate.beforeIndexShardClosed(shardId, indexShard, indexSettings);
        }

        @Override
        public void afterIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
            delegate.afterIndexShardClosed(shardId, indexShard, indexSettings);
        }

        @Override
        public void indexShardStateChanged(IndexShard indexShard, @Nullable IndexShardState previousState,
                IndexShardState currentState, @Nullable String reason) {
            delegate.indexShardStateChanged(indexShard, previousState, currentState, reason);
        }

        @Override
        public void beforeIndexCreated(Index index, Settings indexSettings) {
            delegate.beforeIndexCreated(index, indexSettings);
        }

        @Override
        public void afterIndexCreated(IndexService indexService) {
            delegate.afterIndexCreated(indexService);
        }

        @Override
        public void beforeIndexShardCreated(ShardId shardId, Settings indexSettings) {
            delegate.beforeIndexShardCreated(shardId, indexSettings);
        }

        @Override
        public void beforeIndexRemoved(IndexService indexService, IndexRemovalReason reason) {
            delegate.beforeIndexRemoved(indexService, reason);
        }

        @Override
        public void afterIndexRemoved(Index index, IndexSettings indexSettings, IndexRemovalReason reason) {
            delegate.afterIndexRemoved(index, indexSettings, reason);
        }

        @Override
        public void beforeIndexShardDeleted(ShardId shardId, Settings indexSettings) {
            delegate.beforeIndexShardDeleted(shardId, indexSettings);
        }

        @Override
        public void afterIndexShardDeleted(ShardId shardId, Settings indexSettings) {
            delegate.afterIndexShardDeleted(shardId, indexSettings);
        }

        @Override
        public void beforeIndexAddedToCluster(Index index, Settings indexSettings) {
            delegate.beforeIndexAddedToCluster(index, indexSettings);
        }
    }
}
