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
package org.elasticsearch.env;

import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.gateway.MetaDataStateFormat;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;

@LuceneTestCase.SuppressFileSystems("ExtrasFS") // TODO: fix test to allow extras
public class NodeEnvironmentTests extends ESTestCase {
    private final IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("foo", Settings.EMPTY);

    public void testNodeLockSillySettings() {
        try {
            NodeEnvironment.MAX_LOCAL_STORAGE_NODES_SETTING.get(Settings.builder()
                    .put(NodeEnvironment.MAX_LOCAL_STORAGE_NODES_SETTING.getKey(), between(Integer.MIN_VALUE, 0)).build());
            fail("expected failure");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("must be >= 1"));
        }

        // Even though its silly MAXINT nodes is a-ok!
        int value = between(1, Integer.MAX_VALUE);
        int max = NodeEnvironment.MAX_LOCAL_STORAGE_NODES_SETTING.get(
                Settings.builder().put(NodeEnvironment.MAX_LOCAL_STORAGE_NODES_SETTING.getKey(), value).build());
        assertEquals(value, max);
    }

    public void testNodeLockSingleEnvironment() throws IOException {
        final Settings settings = buildEnvSettings(Settings.builder().put("node.max_local_storage_nodes", 1).build());
        NodeEnvironment env = newNodeEnvironment(settings);
        List<String> dataPaths = Environment.PATH_DATA_SETTING.get(settings);

        // Reuse the same location and attempt to lock again
        IllegalStateException ex =
            expectThrows(IllegalStateException.class, () -> new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings)));
        assertThat(ex.getMessage(), containsString("failed to obtain node lock"));

        // Close the environment that holds the lock and make sure we can get the lock after release
        env.close();
        env = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
        assertThat(env.nodeDataPaths(), arrayWithSize(dataPaths.size()));

        for (int i = 0; i < dataPaths.size(); i++) {
            assertTrue(env.nodeDataPaths()[i].startsWith(PathUtils.get(dataPaths.get(i))));
        }
        env.close();
        assertThat(env.lockedShards(), empty());
    }

    @SuppressForbidden(reason = "System.out.*")
    public void testSegmentInfosTracing() {
        // Defaults to not hooking up std out
        assertNull(SegmentInfos.getInfoStream());

        try {
            // False means don't hook up std out
            NodeEnvironment.applySegmentInfosTrace(
                    Settings.builder().put(NodeEnvironment.ENABLE_LUCENE_SEGMENT_INFOS_TRACE_SETTING.getKey(), false).build());
            assertNull(SegmentInfos.getInfoStream());

            // But true means hook std out up statically
            NodeEnvironment.applySegmentInfosTrace(
                    Settings.builder().put(NodeEnvironment.ENABLE_LUCENE_SEGMENT_INFOS_TRACE_SETTING.getKey(), true).build());
            assertEquals(System.out, SegmentInfos.getInfoStream());
        } finally {
            // Clean up after ourselves
            SegmentInfos.setInfoStream(null);
        }
    }

    public void testNodeLockMultipleEnvironment() throws IOException {
        final Settings settings = buildEnvSettings(Settings.builder().put("node.max_local_storage_nodes", 2).build());
        final NodeEnvironment first = newNodeEnvironment(settings);
        List<String> dataPaths = Environment.PATH_DATA_SETTING.get(settings);
        NodeEnvironment second = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
        assertEquals(first.nodeDataPaths().length, dataPaths.size());
        assertEquals(second.nodeDataPaths().length, dataPaths.size());
        for (int i = 0; i < dataPaths.size(); i++) {
            assertEquals(first.nodeDataPaths()[i].getParent(), second.nodeDataPaths()[i].getParent());
        }
        IOUtils.close(first, second);
    }

    public void testShardLock() throws Exception {
        final NodeEnvironment env = newNodeEnvironment();

        Index index = new Index("foo", "fooUUID");
        ShardLock fooLock = env.shardLock(new ShardId(index, 0));
        assertEquals(new ShardId(index, 0), fooLock.getShardId());

        try {
            env.shardLock(new ShardId(index, 0));
            fail("shard is locked");
        } catch (ShardLockObtainFailedException ex) {
            // expected
        }
        for (Path path : env.indexPaths(index)) {
            Files.createDirectories(path.resolve("0"));
            Files.createDirectories(path.resolve("1"));
        }
        try {
            env.lockAllForIndex(index, idxSettings, randomIntBetween(0, 10));
            fail("shard 0 is locked");
        } catch (ShardLockObtainFailedException ex) {
            // expected
        }

        fooLock.close();
        // can lock again?
        env.shardLock(new ShardId(index, 0)).close();

        List<ShardLock> locks = env.lockAllForIndex(index, idxSettings, randomIntBetween(0, 10));
        try {
            env.shardLock(new ShardId(index, 0));
            fail("shard is locked");
        } catch (ShardLockObtainFailedException ex) {
            // expected
        }
        IOUtils.close(locks);
        assertTrue("LockedShards: " + env.lockedShards(), env.lockedShards().isEmpty());
        env.close();
    }

    public void testAvailableIndexFolders() throws Exception {
        final NodeEnvironment env = newNodeEnvironment();
        final int numIndices = randomIntBetween(1, 10);
        Set<String> actualPaths = new HashSet<>();
        for (int i = 0; i < numIndices; i++) {
            Index index = new Index("foo" + i, "fooUUID" + i);
            for (Path path : env.indexPaths(index)) {
                Files.createDirectories(path.resolve(MetaDataStateFormat.STATE_DIR_NAME));
                actualPaths.add(path.getFileName().toString());
            }
        }

        assertThat(actualPaths, equalTo(env.availableIndexFolders()));
        assertTrue("LockedShards: " + env.lockedShards(), env.lockedShards().isEmpty());
        env.close();
    }

    public void testResolveIndexFolders() throws Exception {
        final NodeEnvironment env = newNodeEnvironment();
        final int numIndices = randomIntBetween(1, 10);
        Map<String, List<Path>> actualIndexDataPaths = new HashMap<>();
        for (int i = 0; i < numIndices; i++) {
            Index index = new Index("foo" + i, "fooUUID" + i);
            Path[] indexPaths = env.indexPaths(index);
            for (Path path : indexPaths) {
                Files.createDirectories(path);
                String fileName = path.getFileName().toString();
                List<Path> paths = actualIndexDataPaths.get(fileName);
                if (paths == null) {
                    paths = new ArrayList<>();
                }
                paths.add(path);
                actualIndexDataPaths.put(fileName, paths);
            }
        }
        for (Map.Entry<String, List<Path>> actualIndexDataPathEntry : actualIndexDataPaths.entrySet()) {
            List<Path> actual = actualIndexDataPathEntry.getValue();
            Path[] actualPaths = actual.toArray(new Path[actual.size()]);
            assertThat(actualPaths, equalTo(env.resolveIndexFolder(actualIndexDataPathEntry.getKey())));
        }
        assertTrue("LockedShards: " + env.lockedShards(), env.lockedShards().isEmpty());
        env.close();
    }

    public void testDeleteSafe() throws Exception {
        final NodeEnvironment env = newNodeEnvironment();
        final Index index = new Index("foo", "fooUUID");
        ShardLock fooLock = env.shardLock(new ShardId(index, 0));
        assertEquals(new ShardId(index, 0), fooLock.getShardId());

        for (Path path : env.indexPaths(index)) {
            Files.createDirectories(path.resolve("0"));
            Files.createDirectories(path.resolve("1"));
        }

        try {
            env.deleteShardDirectorySafe(new ShardId(index, 0), idxSettings);
            fail("shard is locked");
        } catch (ShardLockObtainFailedException ex) {
            // expected
        }

        for (Path path : env.indexPaths(index)) {
            assertTrue(Files.exists(path.resolve("0")));
            assertTrue(Files.exists(path.resolve("1")));
        }

        env.deleteShardDirectorySafe(new ShardId(index, 1), idxSettings);

        for (Path path : env.indexPaths(index)) {
            assertTrue(Files.exists(path.resolve("0")));
            assertFalse(Files.exists(path.resolve("1")));
        }

        try {
            env.deleteIndexDirectorySafe(index, randomIntBetween(0, 10), idxSettings);
            fail("shard is locked");
        } catch (ShardLockObtainFailedException ex) {
            // expected
        }
        fooLock.close();

        for (Path path : env.indexPaths(index)) {
            assertTrue(Files.exists(path));
        }

        final AtomicReference<Throwable> threadException = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch blockLatch = new CountDownLatch(1);
        final CountDownLatch start = new CountDownLatch(1);
        if (randomBoolean()) {
            Thread t = new Thread(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    logger.error("unexpected error", e);
                    threadException.set(e);
                    latch.countDown();
                    blockLatch.countDown();
                }

                @Override
                protected void doRun() throws Exception {
                    start.await();
                    try (ShardLock autoCloses = env.shardLock(new ShardId(index, 0))) {
                        blockLatch.countDown();
                        Thread.sleep(randomIntBetween(1, 10));
                    }
                    latch.countDown();
                }
            });
            t.start();
        } else {
            latch.countDown();
            blockLatch.countDown();
        }
        start.countDown();
        blockLatch.await();

        env.deleteIndexDirectorySafe(index, 5000, idxSettings);

        assertNull(threadException.get());

        for (Path path : env.indexPaths(index)) {
            assertFalse(Files.exists(path));
        }
        latch.await();
        assertTrue("LockedShards: " + env.lockedShards(), env.lockedShards().isEmpty());
        env.close();
    }

    public void testStressShardLock() throws IOException, InterruptedException {
        class Int {
            int value = 0;
        }
        final NodeEnvironment env = newNodeEnvironment();
        final int shards = randomIntBetween(2, 10);
        final Int[] counts = new Int[shards];
        final AtomicInteger[] countsAtomic = new AtomicInteger[shards];
        final AtomicInteger[] flipFlop = new AtomicInteger[shards];

        for (int i = 0; i < counts.length; i++) {
            counts[i] = new Int();
            countsAtomic[i] = new AtomicInteger();
            flipFlop[i] = new AtomicInteger();
        }

        Thread[] threads = new Thread[randomIntBetween(2, 5)];
        final CountDownLatch latch = new CountDownLatch(1);
        final int iters = scaledRandomIntBetween(10000, 100000);
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread() {
                @Override
                public void run() {
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        fail(e.getMessage());
                    }
                    for (int i = 0; i < iters; i++) {
                        int shard = randomIntBetween(0, counts.length - 1);
                        try {
                            try (ShardLock autoCloses = env.shardLock(new ShardId("foo", "fooUUID", shard), scaledRandomIntBetween(0, 10))) {
                                counts[shard].value++;
                                countsAtomic[shard].incrementAndGet();
                                assertEquals(flipFlop[shard].incrementAndGet(), 1);
                                assertEquals(flipFlop[shard].decrementAndGet(), 0);
                            }
                        } catch (ShardLockObtainFailedException ex) {
                            // ok
                        }
                    }
                }
            };
            threads[i].start();
        }
        latch.countDown(); // fire the threads up
        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
        }

        assertTrue("LockedShards: " + env.lockedShards(), env.lockedShards().isEmpty());
        for (int i = 0; i < counts.length; i++) {
            assertTrue(counts[i].value > 0);
            assertEquals(flipFlop[i].get(), 0);
            assertEquals(counts[i].value, countsAtomic[i].get());
        }
        env.close();
    }

    public void testCustomDataPaths() throws Exception {
        String[] dataPaths = tmpPaths();
        NodeEnvironment env = newNodeEnvironment(dataPaths, "/tmp", Settings.EMPTY);

        final Settings indexSettings = Settings.builder().put(IndexMetaData.SETTING_INDEX_UUID, "myindexUUID").build();
        IndexSettings s1 = IndexSettingsModule.newIndexSettings("myindex", indexSettings);
        IndexSettings s2 = IndexSettingsModule.newIndexSettings("myindex", Settings.builder().put(indexSettings).put(IndexMetaData.SETTING_DATA_PATH, "/tmp/foo").build());
        Index index = new Index("myindex", "myindexUUID");
        ShardId sid = new ShardId(index, 0);

        assertFalse("no settings should mean no custom data path", s1.hasCustomDataPath());
        assertTrue("settings with path_data should have a custom data path", s2.hasCustomDataPath());

        assertThat(env.availableShardPaths(sid), equalTo(env.availableShardPaths(sid)));
        assertThat(env.resolveCustomLocation(s2, sid), equalTo(PathUtils.get("/tmp/foo/0/" + index.getUUID() + "/0")));

        assertThat("shard paths with a custom data_path should contain only regular paths",
                env.availableShardPaths(sid),
                equalTo(stringsToPaths(dataPaths, "nodes/0/indices/" + index.getUUID() + "/0")));

        assertThat("index paths uses the regular template",
                env.indexPaths(index), equalTo(stringsToPaths(dataPaths, "nodes/0/indices/" + index.getUUID())));

        IndexSettings s3 = new IndexSettings(s2.getIndexMetaData(), Settings.builder().build());

        assertThat(env.availableShardPaths(sid), equalTo(env.availableShardPaths(sid)));
        assertThat(env.resolveCustomLocation(s3, sid), equalTo(PathUtils.get("/tmp/foo/0/" + index.getUUID() + "/0")));

        assertThat("shard paths with a custom data_path should contain only regular paths",
                env.availableShardPaths(sid),
                equalTo(stringsToPaths(dataPaths, "nodes/0/indices/" + index.getUUID() + "/0")));

        assertThat("index paths uses the regular template",
                env.indexPaths(index), equalTo(stringsToPaths(dataPaths, "nodes/0/indices/" + index.getUUID())));

        env.close();
    }

    public void testPersistentNodeId() throws IOException {
        NodeEnvironment env = newNodeEnvironment(new String[0], Settings.builder()
            .put("node.local_storage", false)
            .put("node.master", false)
            .put("node.data", false)
            .build());
        String nodeID = env.nodeId();
        env.close();
        final String[] paths = tmpPaths();
        env = newNodeEnvironment(paths, Settings.EMPTY);
        assertThat("previous node didn't have local storage enabled, id should change", env.nodeId(), not(equalTo(nodeID)));
        nodeID = env.nodeId();
        env.close();
        env = newNodeEnvironment(paths, Settings.EMPTY);
        assertThat(env.nodeId(), equalTo(nodeID));
        env.close();
        env = newNodeEnvironment(Settings.EMPTY);
        assertThat(env.nodeId(), not(equalTo(nodeID)));
        env.close();
    }

    public void testExistingTempFiles() throws IOException {
        String[] paths = tmpPaths();
        // simulate some previous left over temp files
        for (String path : randomSubsetOf(randomIntBetween(1, paths.length), paths)) {
            final Path nodePath = NodeEnvironment.resolveNodePath(PathUtils.get(path), 0);
            Files.createDirectories(nodePath);
            Files.createFile(nodePath.resolve(NodeEnvironment.TEMP_FILE_NAME));
            if (randomBoolean()) {
                Files.createFile(nodePath.resolve(NodeEnvironment.TEMP_FILE_NAME + ".tmp"));
            }
            if (randomBoolean()) {
                Files.createFile(nodePath.resolve(NodeEnvironment.TEMP_FILE_NAME + ".final"));
            }
        }
        NodeEnvironment env = newNodeEnvironment(paths, Settings.EMPTY);
        try {
            env.ensureAtomicMoveSupported();
        } catch (AtomicMoveNotSupportedException e) {
            // that's OK :)
        }
        env.close();
        // check we clean up
        for (String path: paths) {
            final Path nodePath = NodeEnvironment.resolveNodePath(PathUtils.get(path), 0);
            final Path tempFile = nodePath.resolve(NodeEnvironment.TEMP_FILE_NAME);
            assertFalse(tempFile + " should have been cleaned", Files.exists(tempFile));
            final Path srcTempFile = nodePath.resolve(NodeEnvironment.TEMP_FILE_NAME + ".src");
            assertFalse(srcTempFile + " should have been cleaned", Files.exists(srcTempFile));
            final Path targetTempFile = nodePath.resolve(NodeEnvironment.TEMP_FILE_NAME + ".target");
            assertFalse(targetTempFile + " should have been cleaned", Files.exists(targetTempFile));
        }
    }

    /** Converts an array of Strings to an array of Paths, adding an additional child if specified */
    private Path[] stringsToPaths(String[] strings, String additional) {
        Path[] locations = new Path[strings.length];
        for (int i = 0; i < strings.length; i++) {
            locations[i] = PathUtils.get(strings[i], additional);
        }
        return locations;
    }

    @Override
    public String[] tmpPaths() {
        final int numPaths = randomIntBetween(1, 3);
        final String[] absPaths = new String[numPaths];
        for (int i = 0; i < numPaths; i++) {
            absPaths[i] = createTempDir().toAbsolutePath().toString();
        }
        return absPaths;
    }

    @Override
    public NodeEnvironment newNodeEnvironment() throws IOException {
        return newNodeEnvironment(Settings.EMPTY);
    }

    @Override
    public NodeEnvironment newNodeEnvironment(Settings settings) throws IOException {
        Settings build = buildEnvSettings(settings);
        return new NodeEnvironment(build, TestEnvironment.newEnvironment(build));
    }

    public Settings buildEnvSettings(Settings settings) {
        return Settings.builder()
                    .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath().toString())
                    .putList(Environment.PATH_DATA_SETTING.getKey(), tmpPaths())
                    .put(settings).build();
    }

    public NodeEnvironment newNodeEnvironment(String[] dataPaths, Settings settings) throws IOException {
        Settings build = Settings.builder()
                .put(settings)
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath().toString())
                .putList(Environment.PATH_DATA_SETTING.getKey(), dataPaths).build();
        return new NodeEnvironment(build, TestEnvironment.newEnvironment(build));
    }

    public NodeEnvironment newNodeEnvironment(String[] dataPaths, String sharedDataPath, Settings settings) throws IOException {
        Settings build = Settings.builder()
                .put(settings)
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath().toString())
                .put(Environment.PATH_SHARED_DATA_SETTING.getKey(), sharedDataPath)
                .putList(Environment.PATH_DATA_SETTING.getKey(), dataPaths).build();
        return new NodeEnvironment(build, TestEnvironment.newEnvironment(build));
    }
}
