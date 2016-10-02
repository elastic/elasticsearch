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
package org.elasticsearch.index.shard;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;

import java.io.IOException;
import java.nio.file.Path;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

/**
 */
public class ShardPathTests extends ESTestCase {
    public void testLoadShardPath() throws IOException {
        try (final NodeEnvironment env = newNodeEnvironment(Settings.builder().build())) {
            Settings.Builder builder = Settings.builder().put(IndexMetaData.SETTING_INDEX_UUID, "0xDEADBEEF")
                    .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT);
            Settings settings = builder.build();
            ShardId shardId = new ShardId("foo", "0xDEADBEEF", 0);
            Path[] paths = env.availableShardPaths(shardId);
            Path path = randomFrom(paths);
            ShardStateMetaData.FORMAT.write(new ShardStateMetaData(2, true, "0xDEADBEEF", AllocationId.newInitializing()), path);
            ShardPath shardPath = ShardPath.loadShardPath(logger, env, shardId, IndexSettingsModule.newIndexSettings(shardId.getIndex(), settings));
            assertEquals(path, shardPath.getDataPath());
            assertEquals("0xDEADBEEF", shardPath.getShardId().getIndex().getUUID());
            assertEquals("foo", shardPath.getShardId().getIndexName());
            assertEquals(path.resolve("translog"), shardPath.resolveTranslog());
            assertEquals(path.resolve("index"), shardPath.resolveIndex());
        }
    }

    public void testFailLoadShardPathOnMultiState() throws IOException {
        try (final NodeEnvironment env = newNodeEnvironment(Settings.builder().build())) {
            final String indexUUID = "0xDEADBEEF";
            Settings.Builder builder = Settings.builder().put(IndexMetaData.SETTING_INDEX_UUID, indexUUID)
                    .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT);
            Settings settings = builder.build();
            ShardId shardId = new ShardId("foo", indexUUID, 0);
            Path[] paths = env.availableShardPaths(shardId);
            assumeTrue("This test tests multi data.path but we only got one", paths.length > 1);
            int id = randomIntBetween(1, 10);
            ShardStateMetaData.FORMAT.write(new ShardStateMetaData(id, true, indexUUID, AllocationId.newInitializing()), paths);
            ShardPath.loadShardPath(logger, env, shardId, IndexSettingsModule.newIndexSettings(shardId.getIndex(), settings));
            fail("Expected IllegalStateException");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), containsString("more than one shard state found"));
        }
    }

    public void testFailLoadShardPathIndexUUIDMissmatch() throws IOException {
        try (final NodeEnvironment env = newNodeEnvironment(Settings.builder().build())) {
            Settings.Builder builder = Settings.builder().put(IndexMetaData.SETTING_INDEX_UUID, "foobar")
                    .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT);
            Settings settings = builder.build();
            ShardId shardId = new ShardId("foo", "foobar", 0);
            Path[] paths = env.availableShardPaths(shardId);
            Path path = randomFrom(paths);
            int id = randomIntBetween(1, 10);
            ShardStateMetaData.FORMAT.write(new ShardStateMetaData(id, true, "0xDEADBEEF", AllocationId.newInitializing()), path);
            ShardPath.loadShardPath(logger, env, shardId, IndexSettingsModule.newIndexSettings(shardId.getIndex(), settings));
            fail("Expected IllegalStateException");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), containsString("expected: foobar on shard path"));
        }
    }

    public void testIllegalCustomDataPath() {
        Index index = new Index("foo", "foo");
        final Path path = createTempDir().resolve(index.getUUID()).resolve("0");
        try {
            new ShardPath(true, path, path, new ShardId(index, 0));
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("shard state path must be different to the data path when using custom data paths"));
        }
    }

    public void testValidCtor() {
        Index index = new Index("foo", "foo");
        final Path path = createTempDir().resolve(index.getUUID()).resolve("0");
        ShardPath shardPath = new ShardPath(false, path, path, new ShardId(index, 0));
        assertFalse(shardPath.isCustomDataPath());
        assertEquals(shardPath.getDataPath(), path);
        assertEquals(shardPath.getShardStatePath(), path);
    }

    public void testGetRootPaths() throws IOException {
        boolean useCustomDataPath = randomBoolean();
        final Settings indexSettings;
        final Settings nodeSettings;
        final String indexUUID = "0xDEADBEEF";
        Settings.Builder indexSettingsBuilder = Settings.builder()
                .put(IndexMetaData.SETTING_INDEX_UUID, indexUUID)
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT);
        final Path customPath;
        if (useCustomDataPath) {
            final Path path = createTempDir();
            final boolean includeNodeId = randomBoolean();
            indexSettings = indexSettingsBuilder.put(IndexMetaData.SETTING_DATA_PATH, "custom").build();
            nodeSettings = Settings.builder().put(Environment.PATH_SHARED_DATA_SETTING.getKey(), path.toAbsolutePath().toAbsolutePath())
                    .put(NodeEnvironment.ADD_NODE_LOCK_ID_TO_CUSTOM_PATH.getKey(), includeNodeId).build();
            if (includeNodeId) {
                customPath = path.resolve("custom").resolve("0");
            } else {
                customPath = path.resolve("custom");
            }
        } else {
            customPath = null;
            indexSettings = indexSettingsBuilder.build();
            nodeSettings = Settings.EMPTY;
        }
        try (final NodeEnvironment env = newNodeEnvironment(nodeSettings)) {
            ShardId shardId = new ShardId("foo", indexUUID, 0);
            Path[] paths = env.availableShardPaths(shardId);
            Path path = randomFrom(paths);
            ShardStateMetaData.FORMAT.write(new ShardStateMetaData(2, true, indexUUID, AllocationId.newInitializing()), path);
            ShardPath shardPath = ShardPath.loadShardPath(logger, env, shardId,
                IndexSettingsModule.newIndexSettings(shardId.getIndex(), indexSettings, nodeSettings));
            boolean found = false;
            for (Path p : env.nodeDataPaths()) {
                if (p.equals(shardPath.getRootStatePath())) {
                    found = true;
                    break;
                }
            }
            assertTrue("root state paths must be a node path but wasn't: " + shardPath.getRootStatePath(), found);
            found = false;
            if (useCustomDataPath) {
                assertNotEquals(shardPath.getRootDataPath(), shardPath.getRootStatePath());
                assertEquals(customPath, shardPath.getRootDataPath());
            } else {
                assertNull(customPath);
                for (Path p : env.nodeDataPaths()) {
                    if (p.equals(shardPath.getRootDataPath())) {
                        found = true;
                        break;
                    }
                }
                assertTrue("root state paths must be a node path but wasn't: " + shardPath.getRootDataPath(), found);
            }
        }
    }

}
