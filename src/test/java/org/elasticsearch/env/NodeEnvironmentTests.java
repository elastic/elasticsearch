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
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class NodeEnvironmentTests extends ElasticsearchTestCase {

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
            assertTrue(env.nodeDataPaths()[i].startsWith(Paths.get(dataPaths[i])));
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

        ShardLock fooLock = env.shardLock(new ShardId("foo", 1));
        assertEquals(new ShardId("foo", 1), fooLock.getShardId());

        try {
            env.shardLock(new ShardId("foo", 1));
            fail("shard is locked");
        } catch (LockObtainFailedException ex) {
            // expected
        }
        for (Path path : env.indexPaths(new Index("foo"))) {
            Files.createDirectories(path.resolve("1"));
            Files.createDirectories(path.resolve("2"));
        }

        try {
            env.lockAllForIndex(new Index("foo"));
            fail("shard 1 is locked");
        } catch (LockObtainFailedException ex) {
            // expected
        }

        fooLock.close();
        // can lock again?
        env.shardLock(new ShardId("foo", 1)).close();

        List<ShardLock> locks = env.lockAllForIndex(new Index("foo"));
        try {
            env.shardLock(new ShardId("foo", randomBoolean() ? 1 : 2));
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
    public void testDeleteSafe() throws IOException {
        final NodeEnvironment env = newNodeEnvironment();
        ShardLock fooLock = env.shardLock(new ShardId("foo", 1));
        assertEquals(new ShardId("foo", 1), fooLock.getShardId());


        for (Path path : env.indexPaths(new Index("foo"))) {
            Files.createDirectories(path.resolve("1"));
            Files.createDirectories(path.resolve("2"));
        }

        try {
            env.deleteShardDirectorySafe(new ShardId("foo", 1));
            fail("shard is locked");
        } catch (LockObtainFailedException ex) {
            // expected
        }

        for (Path path : env.indexPaths(new Index("foo"))) {
            assertTrue(Files.exists(path.resolve("1")));
            assertTrue(Files.exists(path.resolve("2")));

        }

        env.deleteShardDirectorySafe(new ShardId("foo", 2));

        for (Path path : env.indexPaths(new Index("foo"))) {
            assertTrue(Files.exists(path.resolve("1")));
            assertFalse(Files.exists(path.resolve("2")));
        }

        try {
            env.deleteIndexDirectorySafe(new Index("foo"));
            fail("shard is locked");
        } catch (LockObtainFailedException ex) {
            // expected
        }
        fooLock.close();

        for (Path path : env.indexPaths(new Index("foo"))) {
            assertTrue(Files.exists(path));
        }

        env.deleteIndexDirectorySafe(new Index("foo"));

        for (Path path : env.indexPaths(new Index("foo"))) {
            assertFalse(Files.exists(path));
        }
        assertTrue("LockedShards: " + env.lockedShards(), env.lockedShards().isEmpty());
        env.close();
    }

    @Test
    public void testGetAllShards() throws Exception {
        final NodeEnvironment env = newNodeEnvironment();
        final int numIndices = randomIntBetween(1, 10);
        final Set<ShardId> createdShards = new HashSet<>();
        for (int i = 0; i < numIndices; i++) {
            for (Path path : env.indexPaths(new Index("foo" + i))) {
                final int numShards = randomIntBetween(1, 10);
                for (int j = 0; j < numShards; j++) {
                    Files.createDirectories(path.resolve(Integer.toString(j)));
                    createdShards.add(new ShardId("foo" + i, j));
                }
            }
        }
        Set<ShardId> shards = env.findAllShardIds();
        assertEquals(shards.size(), createdShards.size());
        assertEquals(shards, createdShards);

        Index index = new Index("foo" + randomIntBetween(1, numIndices));
        shards = env.findAllShardIds(index);
        for (ShardId id : createdShards) {
            if (index.getName().equals(id.getIndex())) {
                assertNotNull("missing shard " + id, shards.remove(id));
            }
        }
        assertEquals("too many shards found", shards.size(), 0);
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

    public NodeEnvironment newNodeEnvironment() throws IOException {
        return newNodeEnvironment(ImmutableSettings.EMPTY);
    }

    public NodeEnvironment newNodeEnvironment(Settings settings) throws IOException {
        Settings build = ImmutableSettings.builder()
                .put(settings)
                .put("path.home", newTempDir().getAbsolutePath())
                .putArray("path.data", tmpPaths()).build();
        return new NodeEnvironment(build, new Environment(build));
    }


}
