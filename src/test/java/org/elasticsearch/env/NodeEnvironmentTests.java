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

import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.CoreMatchers.equalTo;

@LuceneTestCase.SuppressFileSystems("ExtrasFS") // TODO: fix test to allow extras
public class NodeEnvironmentTests extends ElasticsearchTestCase {

    private final Settings idxSettings = ImmutableSettings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).build();

    @Test
    public void testNodeLockSingleEnvironment() throws IOException {
        NodeEnvironment env = newNodeEnvironment(ImmutableSettings.builder()
                .put("node.max_local_storage_nodes", 1).build());
        Settings settings = env.getSettings();
        String[] dataPaths = env.getSettings().getAsArray("path.data");

        try {
            new NodeEnvironment(settings, new Environment(settings));
            fail("env is already locked");
        } catch (ElasticsearchIllegalStateException ex) {

        }
        env.close();

        // now can recreate and lock it
        env = new NodeEnvironment(settings, new Environment(settings));
        assertEquals(env.nodeDataPaths().length, dataPaths.length);

        for (int i = 0; i < dataPaths.length; i++) {
            assertTrue(env.nodeDataPaths()[i].startsWith(PathUtils.get(dataPaths[i])));
        }
        env.close();
        assertTrue("LockedShards: " + env.lockedShards(), env.lockedShards().isEmpty());

    }

    @Test
    public void testNodeLockMultipleEnvironment() throws IOException {
        final NodeEnvironment first = newNodeEnvironment();
        String[] dataPaths = first.getSettings().getAsArray("path.data");
        NodeEnvironment second = new NodeEnvironment(first.getSettings(), new Environment(first.getSettings()));
        assertEquals(first.nodeDataPaths().length, dataPaths.length);
        assertEquals(second.nodeDataPaths().length, dataPaths.length);
        for (int i = 0; i < dataPaths.length; i++) {
            assertEquals(first.nodeDataPaths()[i].getParent(), second.nodeDataPaths()[i].getParent());
        }
        IOUtils.close(first, second);
    }

    @Test
    public void testShardLock() throws IOException {
        final NodeEnvironment env = newNodeEnvironment();

        ShardLock fooLock = env.shardLock(new ShardId("foo", 0));
        assertEquals(new ShardId("foo", 0), fooLock.getShardId());

        try {
            env.shardLock(new ShardId("foo", 0));
            fail("shard is locked");
        } catch (LockObtainFailedException ex) {
            // expected
        }
        for (Path path : env.indexPaths(new Index("foo"))) {
            Files.createDirectories(path.resolve("0"));
            Files.createDirectories(path.resolve("1"));
        }
        Settings settings = settingsBuilder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 10)).build();
        try {
            env.lockAllForIndex(new Index("foo"), settings, randomIntBetween(0, 10));
            fail("shard 0 is locked");
        } catch (LockObtainFailedException ex) {
            // expected
        }

        fooLock.close();
        // can lock again?
        env.shardLock(new ShardId("foo", 0)).close();

        List<ShardLock> locks = env.lockAllForIndex(new Index("foo"), settings, randomIntBetween(0, 10));
        try {
            env.shardLock(new ShardId("foo", 0));
            fail("shard is locked");
        } catch (LockObtainFailedException ex) {
            // expected
        }
        IOUtils.close(locks);
        assertTrue("LockedShards: " + env.lockedShards(), env.lockedShards().isEmpty());
        env.close();
    }

    @Test
    public void testGetAllIndices() throws Exception {
        final NodeEnvironment env = newNodeEnvironment();
        final int numIndices = randomIntBetween(1, 10);
        for (int i = 0; i < numIndices; i++) {
            for (Path path : env.indexPaths(new Index("foo" + i))) {
                Files.createDirectories(path);
            }
        }
        Set<String> indices = env.findAllIndices();
        assertEquals(indices.size(), numIndices);
        for (int i = 0; i < numIndices; i++) {
            assertTrue(indices.contains("foo" + i));
        }
        assertTrue("LockedShards: " + env.lockedShards(), env.lockedShards().isEmpty());
        env.close();
    }

    @Test
    public void testDeleteSafe() throws IOException, InterruptedException {
        final NodeEnvironment env = newNodeEnvironment();
        ShardLock fooLock = env.shardLock(new ShardId("foo", 0));
        assertEquals(new ShardId("foo", 0), fooLock.getShardId());


        for (Path path : env.indexPaths(new Index("foo"))) {
            Files.createDirectories(path.resolve("0"));
            Files.createDirectories(path.resolve("1"));
        }

        try {
            env.deleteShardDirectorySafe(new ShardId("foo", 0), idxSettings);
            fail("shard is locked");
        } catch (LockObtainFailedException ex) {
            // expected
        }

        for (Path path : env.indexPaths(new Index("foo"))) {
            assertTrue(Files.exists(path.resolve("0")));
            assertTrue(Files.exists(path.resolve("1")));

        }

        env.deleteShardDirectorySafe(new ShardId("foo", 1), idxSettings);

        for (Path path : env.indexPaths(new Index("foo"))) {
            assertTrue(Files.exists(path.resolve("0")));
            assertFalse(Files.exists(path.resolve("1")));
        }

        try {
            env.deleteIndexDirectorySafe(new Index("foo"), randomIntBetween(0, 10), idxSettings);
            fail("shard is locked");
        } catch (LockObtainFailedException ex) {
            // expected
        }
        fooLock.close();

        for (Path path : env.indexPaths(new Index("foo"))) {
            assertTrue(Files.exists(path));
        }

        final AtomicReference<Throwable> threadException = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch blockLatch = new CountDownLatch(1);
        final CountDownLatch start = new CountDownLatch(1);
        if (randomBoolean()) {
            Thread t = new Thread(new AbstractRunnable() {
                @Override
                public void onFailure(Throwable t) {
                    logger.error("unexpected error", t);
                    threadException.set(t);
                    latch.countDown();
                    blockLatch.countDown();
                }

                @Override
                protected void doRun() throws Exception {
                    start.await();
                    try (ShardLock _ = env.shardLock(new ShardId("foo", 0))) {
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

        env.deleteIndexDirectorySafe(new Index("foo"), 5000, idxSettings);

        assertNull(threadException.get());

        for (Path path : env.indexPaths(new Index("foo"))) {
            assertFalse(Files.exists(path));
        }
        latch.await();
        assertTrue("LockedShards: " + env.lockedShards(), env.lockedShards().isEmpty());
        env.close();
    }

    @Test
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

        Thread[] threads = new Thread[randomIntBetween(2,5)];
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
                        int shard = randomIntBetween(0, counts.length-1);
                        try {
                            try (ShardLock _ = env.shardLock(new ShardId("foo", shard), scaledRandomIntBetween(0, 10))) {
                                counts[shard].value++;
                                countsAtomic[shard].incrementAndGet();
                                assertEquals(flipFlop[shard].incrementAndGet(), 1);
                                assertEquals(flipFlop[shard].decrementAndGet(), 0);
                            }
                        } catch (LockObtainFailedException ex) {
                            // ok
                        } catch (IOException ex) {
                            fail(ex.toString());
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

    @Test
    public void testCustomDataPaths() throws Exception {
        String[] dataPaths = tmpPaths();
        NodeEnvironment env = newNodeEnvironment(dataPaths, ImmutableSettings.EMPTY);

        Settings s1 = ImmutableSettings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1).build();
        Settings s2 = ImmutableSettings.builder().put(IndexMetaData.SETTING_DATA_PATH, "/tmp/foo").build();
        ShardId sid = new ShardId("myindex", 0);
        Index i = new Index("myindex");

        assertFalse("no settings should mean no custom data path", NodeEnvironment.hasCustomDataPath(s1));
        assertTrue("settings with path_data should have a custom data path", NodeEnvironment.hasCustomDataPath(s2));

        assertThat(env.availableShardPaths(sid), equalTo(env.availableShardPaths(sid)));
        assertFalse(NodeEnvironment.hasCustomDataPath(s1));
        assertThat(env.resolveCustomLocation(s2, sid), equalTo(PathUtils.get("/tmp/foo/0/myindex/0")));
        assertTrue(NodeEnvironment.hasCustomDataPath(s2));

        assertThat("shard paths with a custom data_path should contain only regular paths",
                env.availableShardPaths(sid),
                equalTo(stringsToPaths(dataPaths, "elasticsearch/nodes/0/indices/myindex/0")));

        assertThat("index paths uses the regular template",
                env.indexPaths(i), equalTo(stringsToPaths(dataPaths, "elasticsearch/nodes/0/indices/myindex")));

        env.close();
        NodeEnvironment env2 = newNodeEnvironment(dataPaths,
                ImmutableSettings.builder().put(NodeEnvironment.ADD_NODE_ID_TO_CUSTOM_PATH, false).build());

        assertThat(env2.availableShardPaths(sid), equalTo(env2.availableShardPaths(sid)));
        assertThat(env2.resolveCustomLocation(s2, sid), equalTo(PathUtils.get("/tmp/foo/myindex/0")));

        assertThat("shard paths with a custom data_path should contain only regular paths",
                env2.availableShardPaths(sid),
                equalTo(stringsToPaths(dataPaths, "elasticsearch/nodes/0/indices/myindex/0")));

        assertThat("index paths uses the regular template",
                env2.indexPaths(i), equalTo(stringsToPaths(dataPaths, "elasticsearch/nodes/0/indices/myindex")));

        env2.close();
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
        return newNodeEnvironment(ImmutableSettings.EMPTY);
    }

    @Override
    public NodeEnvironment newNodeEnvironment(Settings settings) throws IOException {
        Settings build = ImmutableSettings.builder()
                .put(settings)
                .put("path.home", createTempDir().toAbsolutePath().toString())
                .put(NodeEnvironment.SETTING_CUSTOM_DATA_PATH_ENABLED, true)
                .putArray("path.data", tmpPaths()).build();
        return new NodeEnvironment(build, new Environment(build));
    }

    public NodeEnvironment newNodeEnvironment(String[] dataPaths, Settings settings) throws IOException {
        Settings build = ImmutableSettings.builder()
                .put(settings)
                .put("path.home", createTempDir().toAbsolutePath().toString())
                .put(NodeEnvironment.SETTING_CUSTOM_DATA_PATH_ENABLED, true)
                .putArray("path.data", dataPaths).build();
        return new NodeEnvironment(build, new Environment(build));
    }
}
