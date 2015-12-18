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

package org.elasticsearch.test.store;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.DirectoryService;
import org.elasticsearch.index.store.IndexStore;
import org.elasticsearch.index.store.IndexStoreConfig;
import org.elasticsearch.plugins.Plugin;

import java.util.Collections;
import java.util.EnumSet;
import java.util.IdentityHashMap;
import java.util.Map;

public class MockFSIndexStore extends IndexStore {

    public static final String CHECK_INDEX_ON_CLOSE = "index.store.mock.check_index_on_close";

    public static class TestPlugin extends Plugin {
        @Override
        public String name() {
            return "mock-index-store";
        }
        @Override
        public String description() {
            return "a mock index store for testing";
        }
        @Override
        public Settings additionalSettings() {
            return Settings.builder().put(IndexModule.STORE_TYPE, "mock").build();
        }

        @Override
        public void onIndexModule(IndexModule indexModule) {
            Settings indexSettings = indexModule.getSettings();
            if ("mock".equals(indexSettings.get(IndexModule.STORE_TYPE))) {
                if (indexSettings.getAsBoolean(CHECK_INDEX_ON_CLOSE, true)) {
                    indexModule.addIndexEventListener(new Listener());
                }
                indexModule.addIndexStore("mock", MockFSIndexStore::new);
            }
        }
    }

    MockFSIndexStore(IndexSettings indexSettings,
                     IndexStoreConfig config) {
        super(indexSettings, config);
    }

    public DirectoryService newDirectoryService(ShardPath path) {
        return new MockFSDirectoryService(indexSettings, this, path);
    }

    private static final EnumSet<IndexShardState> validCheckIndexStates = EnumSet.of(
            IndexShardState.STARTED, IndexShardState.RELOCATED, IndexShardState.POST_RECOVERY
    );
    private static final class Listener implements IndexEventListener {

        private final Map<IndexShard, Boolean> shardSet = Collections.synchronizedMap(new IdentityHashMap<>());
        @Override
        public void afterIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
            if (indexShard != null) {
                Boolean remove = shardSet.remove(indexShard);
                if (remove == Boolean.TRUE) {
                    ESLogger logger = Loggers.getLogger(getClass(), indexShard.indexSettings().getSettings(), indexShard.shardId());
                    MockFSDirectoryService.checkIndex(logger, indexShard.store(), indexShard.shardId());
                }
            }
        }

        @Override
        public void indexShardStateChanged(IndexShard indexShard, @Nullable IndexShardState previousState, IndexShardState currentState, @Nullable String reason) {
            if (currentState == IndexShardState.CLOSED && validCheckIndexStates.contains(previousState) && indexShard.indexSettings().isOnSharedFilesystem() == false) {
               shardSet.put(indexShard, Boolean.TRUE);
            }

        }
    }

}
