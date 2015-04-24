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

import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;

/**
 */
public class ShardPathTests extends ElasticsearchTestCase {

    public void testLoadShardPath() throws IOException {
        try (final NodeEnvironment env = newNodeEnvironment(settingsBuilder().build())) {
            ImmutableSettings.Builder builder = settingsBuilder().put(IndexMetaData.SETTING_UUID, "0xDEADBEEF");
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

    @Test(expected = ElasticsearchIllegalStateException.class)
    public void testFailLoadShardPathOnMultiState() throws IOException {
        try (final NodeEnvironment env = newNodeEnvironment(settingsBuilder().build())) {
            ImmutableSettings.Builder builder = settingsBuilder().put(IndexMetaData.SETTING_UUID, "0xDEADBEEF");
            Settings settings = builder.build();
            ShardId shardId = new ShardId("foo", 0);
            Path[] paths = env.availableShardPaths(shardId);
            assumeTrue("This test tests multi data.path but we only got one", paths.length > 1);
            int id = randomIntBetween(1, 10);
            ShardStateMetaData.FORMAT.write(new ShardStateMetaData(id, true, "0xDEADBEEF"), id, paths);
            ShardPath.loadShardPath(logger, env, shardId, settings);
        }
    }

    @Test(expected = ElasticsearchIllegalStateException.class)
    public void testFailLoadShardPathIndexUUIDMissmatch() throws IOException {
        try (final NodeEnvironment env = newNodeEnvironment(settingsBuilder().build())) {
            ImmutableSettings.Builder builder = settingsBuilder().put(IndexMetaData.SETTING_UUID, "foobar");
            Settings settings = builder.build();
            ShardId shardId = new ShardId("foo", 0);
            Path[] paths = env.availableShardPaths(shardId);
            Path path = randomFrom(paths);
            int id = randomIntBetween(1, 10);
            ShardStateMetaData.FORMAT.write(new ShardStateMetaData(id, true, "0xDEADBEEF"), id, path);
            ShardPath.loadShardPath(logger, env, shardId, settings);
        }
    }

}
