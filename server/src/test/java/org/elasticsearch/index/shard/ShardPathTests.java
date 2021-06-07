/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.shard;

import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.file.Path;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class ShardPathTests extends ESTestCase {
    public void testLoadShardPath() throws IOException {
        try (NodeEnvironment env = newNodeEnvironment(Settings.builder().build())) {
            ShardId shardId = new ShardId("foo", "0xDEADBEEF", 0);
            Path path = env.availableShardPath(shardId);
            ShardStateMetadata.FORMAT.writeAndCleanup(
                    new ShardStateMetadata(true, "0xDEADBEEF", AllocationId.newInitializing()), path);
            ShardPath shardPath = ShardPath.loadShardPath(logger, env, shardId, "");
            assertEquals(path, shardPath.getDataPath());
            assertEquals("0xDEADBEEF", shardPath.getShardId().getIndex().getUUID());
            assertEquals("foo", shardPath.getShardId().getIndexName());
            assertEquals(path.resolve("translog"), shardPath.resolveTranslog());
            assertEquals(path.resolve("index"), shardPath.resolveIndex());
        }
    }

    public void testFailLoadShardPathIndexUUIDMissmatch() throws IOException {
        try (NodeEnvironment env = newNodeEnvironment(Settings.builder().build())) {
            ShardId shardId = new ShardId("foo", "foobar", 0);
            Path path = env.availableShardPath(shardId);
            ShardStateMetadata.FORMAT.writeAndCleanup(
                    new ShardStateMetadata(true, "0xDEADBEEF", AllocationId.newInitializing()), path);
            Exception e = expectThrows(IllegalStateException.class, () -> ShardPath.loadShardPath(logger, env, shardId, ""));
            assertThat(e.getMessage(), containsString("expected: foobar on shard path"));
        }
    }

    public void testIllegalCustomDataPath() {
        Index index = new Index("foo", "foo");
        final Path path = createTempDir().resolve(index.getUUID()).resolve("0");
        Exception e = expectThrows(IllegalArgumentException.class, () ->
            new ShardPath(true, path, path, new ShardId(index, 0)));
        assertThat(e.getMessage(), is("shard state path must be different to the data path when using custom data paths"));
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
        final Settings nodeSettings;
        final String indexUUID = "0xDEADBEEF";
        final Path customPath;
        final String customDataPath;
        if (useCustomDataPath) {
            final Path path = createTempDir();
            customDataPath = "custom";
            nodeSettings = Settings.builder().put(Environment.PATH_SHARED_DATA_SETTING.getKey(), path.toAbsolutePath().toAbsolutePath())
                    .build();
            customPath = path.resolve("custom").resolve("0");
        } else {
            customPath = null;
            customDataPath = "";
            nodeSettings = Settings.EMPTY;
        }
        try (NodeEnvironment env = newNodeEnvironment(nodeSettings)) {
            ShardId shardId = new ShardId("foo", indexUUID, 0);
            Path path = env.availableShardPath(shardId);
            ShardStateMetadata.FORMAT.writeAndCleanup(
                    new ShardStateMetadata(true, indexUUID, AllocationId.newInitializing()), path);
            ShardPath shardPath = ShardPath.loadShardPath(logger, env, shardId, customDataPath);
            assertThat("root state paths must be a node path but wasn't: " + shardPath.getRootStatePath(),
                env.nodeDataPath(), equalTo(shardPath.getRootStatePath()));
            if (useCustomDataPath) {
                assertNotEquals(shardPath.getRootDataPath(), shardPath.getRootStatePath());
                assertEquals(customPath, shardPath.getRootDataPath());
            } else {
                assertNull(customPath);
                assertThat("root state paths must be a node path but wasn't: " + shardPath.getRootDataPath(),
                    env.nodeDataPath(), equalTo(shardPath.getRootDataPath()));
            }
        }
    }

}
