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

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;

/**
 */
public class ShardPathTests extends ESTestCase {

    public void testLoadShardPath() throws IOException {
        try (final NodeEnvironment env = newNodeEnvironment(settingsBuilder().build())) {
            Settings.Builder builder = settingsBuilder().put(IndexMetaData.SETTING_INDEX_UUID, "0xDEADBEEF");
            Settings settings = builder.build();
            ShardId shardId = new ShardId("foo", 0);
            Path[] paths = env.availableShardPaths(shardId);
            Path path = randomFrom(paths);
            ShardStateMetaData.FORMAT.write(new ShardStateMetaData(2, true, "0xDEADBEEF"), 2, path);
            ShardPath shardPath = ShardPath.loadShardPath(logger, env, shardId, settings);
            assertEquals(path, shardPath.getDataPath());
            assertEquals("0xDEADBEEF", shardPath.getIndexUUID());
            assertEquals("foo", shardPath.getShardId().getIndex());
            assertEquals(path.resolve("translog"), shardPath.resolveTranslog());
            assertEquals(path.resolve("index"), shardPath.resolveIndex());
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testFailLoadShardPathOnMultiState() throws IOException {
        try (final NodeEnvironment env = newNodeEnvironment(settingsBuilder().build())) {
            Settings.Builder builder = settingsBuilder().put(IndexMetaData.SETTING_INDEX_UUID, "0xDEADBEEF");
            Settings settings = builder.build();
            ShardId shardId = new ShardId("foo", 0);
            Path[] paths = env.availableShardPaths(shardId);
            assumeTrue("This test tests multi data.path but we only got one", paths.length > 1);
            int id = randomIntBetween(1, 10);
            ShardStateMetaData.FORMAT.write(new ShardStateMetaData(id, true, "0xDEADBEEF"), id, paths);
            ShardPath.loadShardPath(logger, env, shardId, settings);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testFailLoadShardPathIndexUUIDMissmatch() throws IOException {
        try (final NodeEnvironment env = newNodeEnvironment(settingsBuilder().build())) {
            Settings.Builder builder = settingsBuilder().put(IndexMetaData.SETTING_INDEX_UUID, "foobar");
            Settings settings = builder.build();
            ShardId shardId = new ShardId("foo", 0);
            Path[] paths = env.availableShardPaths(shardId);
            Path path = randomFrom(paths);
            int id = randomIntBetween(1, 10);
            ShardStateMetaData.FORMAT.write(new ShardStateMetaData(id, true, "0xDEADBEEF"), id, path);
            ShardPath.loadShardPath(logger, env, shardId, settings);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIllegalCustomDataPath() {
        final Path path = createTempDir().resolve("foo").resolve("0");
        new ShardPath(true, path, path, "foo", new ShardId("foo", 0));
    }

    public void testValidCtor() {
        final Path path = createTempDir().resolve("foo").resolve("0");
        ShardPath shardPath = new ShardPath(false, path, path, "foo", new ShardId("foo", 0));
        assertFalse(shardPath.isCustomDataPath());
        assertEquals(shardPath.getDataPath(), path);
        assertEquals(shardPath.getShardStatePath(), path);
    }

    public void testGetRootPaths() throws IOException {
        boolean useCustomDataPath = randomBoolean();
        final Settings indexSetttings;
        final Settings nodeSettings;
        Settings.Builder indexSettingsBuilder = settingsBuilder().put(IndexMetaData.SETTING_INDEX_UUID, "0xDEADBEEF");
        final Path customPath;
        if (useCustomDataPath) {
            final Path path = createTempDir();
            final boolean includeNodeId = randomBoolean();
            indexSetttings = indexSettingsBuilder.put(IndexMetaData.SETTING_DATA_PATH, "custom").build();
            nodeSettings = settingsBuilder().put("path.shared_data", path.toAbsolutePath().toAbsolutePath())
                    .put(NodeEnvironment.ADD_NODE_ID_TO_CUSTOM_PATH, includeNodeId).build();
            if (includeNodeId) {
                customPath = path.resolve("custom").resolve("0");
            } else {
                customPath = path.resolve("custom");
            }
        } else {
            customPath = null;
            indexSetttings = indexSettingsBuilder.build();
            nodeSettings = Settings.EMPTY;
        }
        try (final NodeEnvironment env = newNodeEnvironment(nodeSettings)) {
            ShardId shardId = new ShardId("foo", 0);
            Path[] paths = env.availableShardPaths(shardId);
            Path path = randomFrom(paths);
            ShardStateMetaData.FORMAT.write(new ShardStateMetaData(2, true, "0xDEADBEEF"), 2, path);
            ShardPath shardPath = ShardPath.loadShardPath(logger, env, shardId, indexSetttings);
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
