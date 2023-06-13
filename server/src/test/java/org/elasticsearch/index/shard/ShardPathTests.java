/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.shard;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.ShardLock;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;

public class ShardPathTests extends ESTestCase {
    public void testLoadShardPath() throws IOException {
        try (NodeEnvironment env = newNodeEnvironment(Settings.builder().build())) {
            ShardId shardId = new ShardId("foo", "0xDEADBEEF", 0);
            Path[] paths = env.availableShardPaths(shardId);
            Path path = randomFrom(paths);
            ShardStateMetadata.FORMAT.writeAndCleanup(
                new ShardStateMetadata(true, "0xDEADBEEF", AllocationId.newInitializing()),
                IndexModule.NODE_STORE_USE_FSYNC.get(env.settings()),
                path
            );
            ShardPath shardPath = ShardPath.loadShardPath(logger, env, shardId, "");
            assertEquals(path, shardPath.getDataPath());
            assertEquals("0xDEADBEEF", shardPath.getShardId().getIndex().getUUID());
            assertEquals("foo", shardPath.getShardId().getIndexName());
            assertEquals(path.resolve("translog"), shardPath.resolveTranslog());
            assertEquals(path.resolve("index"), shardPath.resolveIndex());
        }
    }

    public void testFailLoadShardPathOnMultiState() throws IOException {
        try (NodeEnvironment env = newNodeEnvironment(Settings.builder().build())) {
            final String indexUUID = "0xDEADBEEF";
            ShardId shardId = new ShardId("foo", indexUUID, 0);
            Path[] paths = env.availableShardPaths(shardId);
            assumeTrue("This test tests multi data.path but we only got one", paths.length > 1);
            ShardStateMetadata.FORMAT.writeAndCleanup(
                new ShardStateMetadata(true, indexUUID, AllocationId.newInitializing()),
                IndexModule.NODE_STORE_USE_FSYNC.get(env.settings()),
                paths
            );
            Exception e = expectThrows(IllegalStateException.class, () -> ShardPath.loadShardPath(logger, env, shardId, ""));
            assertThat(e.getMessage(), containsString("more than one shard state found"));
        }
    }

    public void testFailLoadShardPathIndexUUIDMissmatch() throws IOException {
        try (NodeEnvironment env = newNodeEnvironment(Settings.builder().build())) {
            ShardId shardId = new ShardId("foo", "foobar", 0);
            Path[] paths = env.availableShardPaths(shardId);
            Path path = randomFrom(paths);
            ShardStateMetadata.FORMAT.writeAndCleanup(
                new ShardStateMetadata(true, "0xDEADBEEF", AllocationId.newInitializing()),
                IndexModule.NODE_STORE_USE_FSYNC.get(env.settings()),
                path
            );
            Exception e = expectThrows(IllegalStateException.class, () -> ShardPath.loadShardPath(logger, env, shardId, ""));
            assertThat(e.getMessage(), containsString("expected: foobar on shard path"));
        }
    }

    public void testIllegalCustomDataPath() {
        Index index = new Index("foo", "foo");
        final Path path = createTempDir().resolve(index.getUUID()).resolve("0");
        Exception e = expectThrows(IllegalArgumentException.class, () -> new ShardPath(true, path, path, new ShardId(index, 0)));
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
            nodeSettings = Settings.builder()
                .put(Environment.PATH_SHARED_DATA_SETTING.getKey(), path.toAbsolutePath().toAbsolutePath())
                .build();
            customPath = path.resolve("custom").resolve("0");
        } else {
            customPath = null;
            customDataPath = "";
            nodeSettings = Settings.EMPTY;
        }
        try (NodeEnvironment env = newNodeEnvironment(nodeSettings)) {
            ShardId shardId = new ShardId("foo", indexUUID, 0);
            Path[] paths = env.availableShardPaths(shardId);
            Path path = randomFrom(paths);
            ShardStateMetadata.FORMAT.writeAndCleanup(
                new ShardStateMetadata(true, indexUUID, AllocationId.newInitializing()),
                IndexModule.NODE_STORE_USE_FSYNC.get(env.settings()),
                path
            );
            ShardPath shardPath = ShardPath.loadShardPath(logger, env, shardId, customDataPath);
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

    public void testLoadShardMultiPath() throws IOException {
        try (NodeEnvironment env = newNodeEnvironment(Settings.builder().build())) {
            ShardId shardId = new ShardId("foo", "0xDEADBEEF", 0);
            Path[] paths = new Path[4];
            for (int i = 0; i < paths.length; i++) {
                paths[i] = createTempDir();
            }
            Path[] envPaths = env.availableShardPaths(shardId);
            paths[between(0, paths.length - 1)] = envPaths[0];
            ShardStateMetadata.FORMAT.writeAndCleanup(
                new ShardStateMetadata(true, "0xDEADBEEF", AllocationId.newInitializing()),
                IndexModule.NODE_STORE_USE_FSYNC.get(env.settings()),
                envPaths
            );

            // Doesn't matter which of the paths contains shard data, we should be able to load it
            ShardPath shardPath = ShardPath.loadShardPath(logger, shardId, "", paths, env.sharedDataPath());
            assertNotNull(shardPath.getDataPath());
            assertEquals(envPaths[0], shardPath.getDataPath());
            assertEquals("0xDEADBEEF", shardPath.getShardId().getIndex().getUUID());
            assertEquals("foo", shardPath.getShardId().getIndexName());
            assertEquals(envPaths[0].resolve("translog"), shardPath.resolveTranslog());
            assertEquals(envPaths[0].resolve("index"), shardPath.resolveIndex());

            // Ensure we validate all paths regardless of successful load
            Path badPath = createTempDir();
            ShardStateMetadata.FORMAT.writeAndCleanup(
                new ShardStateMetadata(true, "0xDEADF00D", AllocationId.newInitializing()),
                IndexModule.NODE_STORE_USE_FSYNC.get(env.settings()),
                badPath
            );

            Path[] extendedPaths = Arrays.copyOf(paths, paths.length + 1);
            extendedPaths[paths.length] = badPath;
            Exception e = expectThrows(
                IllegalStateException.class,
                () -> ShardPath.loadShardPath(logger, shardId, "", extendedPaths, env.sharedDataPath())
            );
            assertThat(
                e.getMessage(),
                is("[foo][0] index UUID in shard state was: 0xDEADF00D expected: 0xDEADBEEF on shard path: " + badPath)
            );
        }
    }

    public void testLoadEmptyShards() throws IOException {
        ShardId shardId = new ShardId("foo", "0xDEADBEEF", 0);
        Path[] paths = new Path[4];
        for (int i = 0; i < paths.length; i++) {
            paths[i] = createTempDir();
        }

        assertNull(ShardPath.loadShardPath(logger, shardId, "", paths, createTempDir()));
    }

    public void testShardPathSelection() throws IOException {
        try (NodeEnvironment env = newNodeEnvironment(Settings.builder().build())) {
            NodeEnvironment.DataPath[] paths = env.dataPaths();
            assertThat(List.of(paths), hasItem(ShardPath.getPathWithMostFreeSpace(env)));
            ShardId shardId = new ShardId("foo", "0xDEADBEEF", 0);

            Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build();
            IndexSettings idxSettings = IndexSettingsModule.newIndexSettings(randomAlphaOfLengthBetween(1, 10), indexSettings);

            ShardPath shardPath = ShardPath.selectNewPathForShard(env, shardId, idxSettings, 1L, new HashMap<>());
            assertNotNull(shardPath.getDataPath());

            List<Path> indexPaths = new ArrayList<>();
            for (NodeEnvironment.DataPath dataPath : paths) {
                indexPaths.add(dataPath.indicesPath.resolve("0xDEADBEEF").resolve("0"));
            }

            assertThat(indexPaths, hasItem(shardPath.getDataPath()));
            assertEquals("0xDEADBEEF", shardPath.getShardId().getIndex().getUUID());
            assertEquals("foo", shardPath.getShardId().getIndexName());
            assertFalse(shardPath.isCustomDataPath());
        }
    }

    public void testDeleteLeftoverShardDirs() throws IOException {
        try (NodeEnvironment env = newNodeEnvironment(Settings.builder().build())) {
            ShardId shardId = new ShardId("foo", "0xDEADBEEF", 0);
            ShardLock lock = env.shardLock(shardId, "starting shard", TimeUnit.SECONDS.toMillis(5));
            try {
                Path[] envPaths = env.availableShardPaths(shardId);
                ShardStateMetadata.FORMAT.writeAndCleanup(
                    new ShardStateMetadata(true, "0xDEADBEEF", AllocationId.newInitializing()),
                    IndexModule.NODE_STORE_USE_FSYNC.get(env.settings()),
                    envPaths
                );

                Path badPath = createTempDir();
                // Cause a failure by writing metadata with UUID that doesn't match
                ShardStateMetadata.FORMAT.writeAndCleanup(
                    new ShardStateMetadata(true, "0xDEADF00D", AllocationId.newInitializing()),
                    IndexModule.NODE_STORE_USE_FSYNC.get(env.settings()),
                    badPath
                );

                List<Path> temp = new ArrayList<>(Arrays.asList(envPaths));
                temp.add(0, badPath);
                Path[] paths = temp.toArray(new Path[0]);

                Exception e = expectThrows(
                    IllegalStateException.class,
                    () -> ShardPath.loadShardPath(logger, shardId, "", paths, env.sharedDataPath())
                );
                assertThat(
                    e.getMessage(),
                    is("[foo][0] index UUID in shard state was: 0xDEADF00D expected: 0xDEADBEEF on shard path: " + badPath)
                );

                Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build();
                IndexSettings idxSettings = IndexSettingsModule.newIndexSettings(randomAlphaOfLengthBetween(1, 10), indexSettings);

                for (Path path : envPaths) {
                    assertTrue(Files.exists(path));
                }
                ShardPath.deleteLeftoverShardDirectory(logger, env, lock, idxSettings, shardPaths -> {
                    List<Path> envPathList = Arrays.asList(envPaths);
                    for (Path path : shardPaths) {
                        assertThat(envPathList, hasItem(path));
                    }
                });
                for (Path path : envPaths) {
                    assertFalse(Files.exists(path));
                }
            } finally {
                IOUtils.closeWhileHandlingException(lock);
            }
        }
    }
}
