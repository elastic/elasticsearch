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

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.IndexStorePlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

public final class MockFSIndexStore {

    public static final Setting<Boolean> INDEX_CHECK_INDEX_ON_CLOSE_SETTING =
        Setting.boolSetting("index.store.mock.check_index_on_close", true, Property.IndexScope, Property.NodeScope);

    public static class TestPlugin extends Plugin implements IndexStorePlugin {
        @Override
        public Settings additionalSettings() {
            return Settings.builder().put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), "mock").build();
        }

        @Override
        public List<Setting<?>> getSettings() {
            return Arrays.asList(INDEX_CHECK_INDEX_ON_CLOSE_SETTING,
            MockFSDirectoryFactory.CRASH_INDEX_SETTING,
            MockFSDirectoryFactory.RANDOM_IO_EXCEPTION_RATE_SETTING,
            MockFSDirectoryFactory.RANDOM_IO_EXCEPTION_RATE_ON_OPEN_SETTING);
        }

        @Override
        public Map<String, DirectoryFactory> getDirectoryFactories() {
            return Collections.singletonMap("mock", new MockFSDirectoryFactory());
        }

        @Override
        public void onIndexModule(IndexModule indexModule) {
            Settings indexSettings = indexModule.getSettings();
            if ("mock".equals(indexSettings.get(IndexModule.INDEX_STORE_TYPE_SETTING.getKey()))) {
                if (INDEX_CHECK_INDEX_ON_CLOSE_SETTING.get(indexSettings)) {
                    indexModule.addIndexEventListener(new Listener());
                }
            }
        }
    }

    private static final EnumSet<IndexShardState> validCheckIndexStates = EnumSet.of(
            IndexShardState.STARTED, IndexShardState.POST_RECOVERY
    );
    private static final class Listener implements IndexEventListener {

        private final Map<IndexShard, Boolean> shardSet = Collections.synchronizedMap(new IdentityHashMap<>());
        @Override
        public void afterIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
            if (indexShard != null) {
                Boolean remove = shardSet.remove(indexShard);
                if (remove == Boolean.TRUE) {
                    Logger logger = Loggers.getLogger(getClass(), indexShard.shardId());
                    MockFSDirectoryFactory.checkIndex(logger, indexShard.store(), indexShard.shardId());
                }
            }
        }

        @Override
        public void indexShardStateChanged(IndexShard indexShard, @Nullable IndexShardState previousState,
                IndexShardState currentState, @Nullable String reason) {
            if (currentState == IndexShardState.CLOSED && validCheckIndexStates.contains(previousState)) {
               shardSet.put(indexShard, Boolean.TRUE);
            }

        }
    }
}
