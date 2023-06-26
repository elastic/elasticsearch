/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.env;

import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.gateway.MetadataStateFormat;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.NodeRoles;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
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
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.env.NodeEnvironment.NODES_FOLDER;
import static org.elasticsearch.test.NodeRoles.nonDataNode;
import static org.elasticsearch.test.NodeRoles.nonMasterNode;
import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

@LuceneTestCase.SuppressFileSystems("ExtrasFS") // TODO: fix test to allow extras
public class NodeEnvironmentTests extends ESTestCase {
    private final IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("foo", Settings.EMPTY);

    public void testNodeLockSillySettings() {
        try {
            NodeEnvironment.MAX_LOCAL_STORAGE_NODES_SETTING.get(
                Settings.builder().put(NodeEnvironment.MAX_LOCAL_STORAGE_NODES_SETTING.getKey(), between(Integer.MIN_VALUE, 0)).build()
            );
            fail("expected failure");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("must be >= 1"));
        }

        // Even though its silly MAXINT nodes is a-ok!
        int value = between(1, Integer.MAX_VALUE);
        int max = NodeEnvironment.MAX_LOCAL_STORAGE_NODES_SETTING.get(
            Settings.builder().put(NodeEnvironment.MAX_LOCAL_STORAGE_NODES_SETTING.getKey(), value).build()
        );
        assertEquals(value, max);
        assertWarnings(
            "[node.max_local_storage_nodes] setting was deprecated in Elasticsearch and will be removed in a future release! "
                + "See the breaking changes documentation for the next major version."
        );
    }

    public void testNodeLockSingleEnvironment() throws IOException {
        final Settings settings = buildEnvSettings(Settings.builder().put("node.max_local_storage_nodes", 1).build());
        NodeEnvironment env = newNodeEnvironment(settings);
        List<String> dataPaths = Environment.PATH_DATA_SETTING.get(settings);

        // Reuse the same location and attempt to lock again
        IllegalStateException ex = expectThrows(
            IllegalStateException.class,
            () -> new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings))
        );
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
        assertWarnings(
            "[node.max_local_storage_nodes] setting was deprecated in Elasticsearch and will be removed in a future release! "
                + "See the breaking changes documentation for the next major version."
        );
    }

    @SuppressForbidden(reason = "System.out.*")
    public void testSegmentInfosTracing() {
        // Defaults to not hooking up std out
        assertNull(SegmentInfos.getInfoStream());

        try {
            // False means don't hook up std out
            NodeEnvironment.applySegmentInfosTrace(
                Settings.builder().put(NodeEnvironment.ENABLE_LUCENE_SEGMENT_INFOS_TRACE_SETTING.getKey(), false).build()
            );
            assertNull(SegmentInfos.getInfoStream());

            // But true means hook std out up statically
            NodeEnvironment.applySegmentInfosTrace(
                Settings.builder().put(NodeEnvironment.ENABLE_LUCENE_SEGMENT_INFOS_TRACE_SETTING.getKey(), true).build()
            );
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
        assertWarnings(
            "[node.max_local_storage_nodes] setting was deprecated in Elasticsearch and will be removed in a future release! "
                + "See the breaking changes documentation for the next major version."
        );
    }

    public void testShardLock() throws Exception {
        final NodeEnvironment env = newNodeEnvironment();

        Index index = new Index("foo", "fooUUID");
        ShardLock fooLock = env.shardLock(new ShardId(index, 0), "1");
        assertEquals(new ShardId(index, 0), fooLock.getShardId());

        try {
            env.shardLock(new ShardId(index, 0), "2");
            fail("shard is locked");
        } catch (ShardLockObtainFailedException ex) {
            // expected
        }
        for (Path path : env.indexPaths(index)) {
            Files.createDirectories(path.resolve("0"));
            Files.createDirectories(path.resolve("1"));
        }
        try {
            env.lockAllForIndex(index, idxSettings, "3", randomIntBetween(0, 10));
            fail("shard 0 is locked");
        } catch (ShardLockObtainFailedException ex) {
            // expected
        }

        fooLock.close();
        // can lock again?
        env.shardLock(new ShardId(index, 0), "4").close();

        List<ShardLock> locks = env.lockAllForIndex(index, idxSettings, "5", randomIntBetween(0, 10));
        try {
            env.shardLock(new ShardId(index, 0), "6");
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
                Files.createDirectories(path.resolve(MetadataStateFormat.STATE_DIR_NAME));
                actualPaths.add(path.getFileName().toString());
            }
        }

        assertThat(actualPaths, equalTo(env.availableIndexFolders()));
        assertTrue("LockedShards: " + env.lockedShards(), env.lockedShards().isEmpty());
        env.close();
    }

    public void testAvailableIndexFoldersWithExclusions() throws Exception {
        final NodeEnvironment env = newNodeEnvironment();
        final int numIndices = randomIntBetween(1, 10);
        Set<String> excludedPaths = new HashSet<>();
        Set<String> actualPaths = new HashSet<>();
        for (int i = 0; i < numIndices; i++) {
            Index index = new Index("foo" + i, "fooUUID" + i);
            for (Path path : env.indexPaths(index)) {
                Files.createDirectories(path.resolve(MetadataStateFormat.STATE_DIR_NAME));
                actualPaths.add(path.getFileName().toString());
            }
            if (randomBoolean()) {
                excludedPaths.add(env.indexPaths(index)[0].getFileName().toString());
            }
        }

        assertThat(Sets.difference(actualPaths, excludedPaths), equalTo(env.availableIndexFolders(excludedPaths::contains)));
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
        final ShardLock fooLock = env.shardLock(new ShardId(index, 0), "1");
        assertEquals(new ShardId(index, 0), fooLock.getShardId());

        for (Path path : env.indexPaths(index)) {
            Files.createDirectories(path.resolve("0"));
            Files.createDirectories(path.resolve("1"));
        }

        expectThrows(
            ShardLockObtainFailedException.class,
            () -> env.deleteShardDirectorySafe(
                new ShardId(index, 0),
                idxSettings,
                shardPaths -> { assert false : "should not be called " + shardPaths; }
            )
        );

        for (Path path : env.indexPaths(index)) {
            assertTrue(Files.exists(path.resolve("0")));
            assertTrue(Files.exists(path.resolve("1")));
        }

        {
            SetOnce<Path[]> listener = new SetOnce<>();
            env.deleteShardDirectorySafe(new ShardId(index, 1), idxSettings, listener::set);
            Path[] deletedPaths = listener.get();
            for (int i = 0; i < env.dataPaths().length; i++) {
                assertThat(deletedPaths[i], equalTo(env.dataPaths()[i].resolve(index).resolve("1")));
            }
        }

        for (Path path : env.indexPaths(index)) {
            assertTrue(Files.exists(path.resolve("0")));
            assertFalse(Files.exists(path.resolve("1")));
        }

        expectThrows(
            ShardLockObtainFailedException.class,
            () -> env.deleteIndexDirectorySafe(
                index,
                randomIntBetween(0, 10),
                idxSettings,
                indexPaths -> { assert false : "should not be called " + indexPaths; }
            )
        );

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
                    try (ShardLock autoCloses = env.shardLock(new ShardId(index, 0), "2")) {
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

        final SetOnce<Path[]> listener = new SetOnce<>();
        env.deleteIndexDirectorySafe(index, 5000, idxSettings, listener::set);
        assertArrayEquals(env.indexPaths(index), listener.get());
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
                            try (
                                ShardLock autoCloses = env.shardLock(
                                    new ShardId("foo", "fooUUID", shard),
                                    "1",
                                    scaledRandomIntBetween(0, 10)
                                )
                            ) {
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

        Index index = new Index("myindex", "myindexUUID");
        ShardId sid = new ShardId(index, 0);

        assertThat(env.availableShardPaths(sid), equalTo(env.availableShardPaths(sid)));
        assertThat(
            env.resolveCustomLocation("/tmp/foo", sid).toAbsolutePath(),
            equalTo(PathUtils.get("/tmp/foo/0/" + index.getUUID() + "/0").toAbsolutePath())
        );

        assertThat(
            "shard paths with a custom data_path should contain only regular paths",
            env.availableShardPaths(sid),
            equalTo(stringsToPaths(dataPaths, "nodes/0/indices/" + index.getUUID() + "/0"))
        );

        assertThat(
            "index paths uses the regular template",
            env.indexPaths(index),
            equalTo(stringsToPaths(dataPaths, "nodes/0/indices/" + index.getUUID()))
        );

        assertThat(env.availableShardPaths(sid), equalTo(env.availableShardPaths(sid)));
        assertThat(
            env.resolveCustomLocation("/tmp/foo", sid).toAbsolutePath(),
            equalTo(PathUtils.get("/tmp/foo/0/" + index.getUUID() + "/0").toAbsolutePath())
        );

        assertThat(
            "shard paths with a custom data_path should contain only regular paths",
            env.availableShardPaths(sid),
            equalTo(stringsToPaths(dataPaths, "nodes/0/indices/" + index.getUUID() + "/0"))
        );

        assertThat(
            "index paths uses the regular template",
            env.indexPaths(index),
            equalTo(stringsToPaths(dataPaths, "nodes/0/indices/" + index.getUUID()))
        );

        env.close();
    }

    public void testNodeIdNotPersistedAtInitialization() throws IOException {
        NodeEnvironment env = newNodeEnvironment(
            new String[0],
            nonMasterNode(nonDataNode(Settings.builder().put("node.local_storage", false).build()))
        );
        String nodeID = env.nodeId();
        env.close();
        final String[] paths = tmpPaths();
        env = newNodeEnvironment(paths, Settings.EMPTY);
        assertThat("previous node didn't have local storage enabled, id should change", env.nodeId(), not(equalTo(nodeID)));
        nodeID = env.nodeId();
        env.close();
        env = newNodeEnvironment(paths, Settings.EMPTY);
        assertThat(env.nodeId(), not(equalTo(nodeID)));
        env.close();
        env = newNodeEnvironment(Settings.EMPTY);
        assertThat(env.nodeId(), not(equalTo(nodeID)));
        env.close();
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { Node.NODE_LOCAL_STORAGE_SETTING });
    }

    public void testExistingTempFiles() throws IOException {
        String[] paths = tmpPaths();
        // simulate some previous left over temp files
        for (String path : randomSubsetOf(randomIntBetween(1, paths.length), paths)) {
            final Path dataPath = NodeEnvironment.resolveDataPath(PathUtils.get(path), 0);
            Files.createDirectories(dataPath);
            Files.createFile(dataPath.resolve(NodeEnvironment.TEMP_FILE_NAME));
            if (randomBoolean()) {
                Files.createFile(dataPath.resolve(NodeEnvironment.TEMP_FILE_NAME + ".tmp"));
            }
            if (randomBoolean()) {
                Files.createFile(dataPath.resolve(NodeEnvironment.TEMP_FILE_NAME + ".final"));
            }
        }
        NodeEnvironment env = newNodeEnvironment(paths, Settings.EMPTY);
        env.close();

        // check we clean up
        for (String path : paths) {
            final Path dataPath = NodeEnvironment.resolveDataPath(PathUtils.get(path), 0);
            final Path tempFile = dataPath.resolve(NodeEnvironment.TEMP_FILE_NAME);
            assertFalse(tempFile + " should have been cleaned", Files.exists(tempFile));
            final Path srcTempFile = dataPath.resolve(NodeEnvironment.TEMP_FILE_NAME + ".src");
            assertFalse(srcTempFile + " should have been cleaned", Files.exists(srcTempFile));
            final Path targetTempFile = dataPath.resolve(NodeEnvironment.TEMP_FILE_NAME + ".target");
            assertFalse(targetTempFile + " should have been cleaned", Files.exists(targetTempFile));
        }
    }

    public void testEnsureNoShardDataOrIndexMetadata() throws IOException {
        Settings settings = buildEnvSettings(Settings.EMPTY);
        Index index = new Index("test", "testUUID");

        // build settings using same path.data as original but without data and master roles
        Settings noDataNoMasterSettings = Settings.builder()
            .put(settings)
            .put(NodeRoles.removeRoles(nonDataNode(settings), org.elasticsearch.core.Set.of(DiscoveryNodeRole.MASTER_ROLE)))
            .build();

        // test that we can create data=false and master=false with no meta information
        newNodeEnvironment(noDataNoMasterSettings).close();

        Path indexPath;
        try (NodeEnvironment env = newNodeEnvironment(settings)) {
            for (Path path : env.indexPaths(index)) {
                Files.createDirectories(path.resolve(MetadataStateFormat.STATE_DIR_NAME));
            }
            indexPath = env.indexPaths(index)[0];
        }

        verifyFailsOnMetadata(noDataNoMasterSettings, indexPath);

        // build settings using same path.data as original but without data role
        Settings noDataSettings = nonDataNode(settings);

        String shardDataDirName = Integer.toString(randomInt(10));

        // test that we can create data=false env with only meta information. Also create shard data for following asserts
        try (NodeEnvironment env = newNodeEnvironment(noDataSettings)) {
            for (Path path : env.indexPaths(index)) {
                Files.createDirectories(path.resolve(shardDataDirName));
            }
        }

        verifyFailsOnShardData(noDataSettings, indexPath, shardDataDirName);

        // assert that we get the stricter message on meta-data when both conditions fail
        verifyFailsOnMetadata(noDataNoMasterSettings, indexPath);

        // build settings using same path.data as original but without master role
        Settings noMasterSettings = nonMasterNode(settings);

        // test that we can create master=false env regardless of data.
        newNodeEnvironment(noMasterSettings).close();

        // test that we can create data=true, master=true env. Also remove state dir to leave only shard data for following asserts
        try (NodeEnvironment env = newNodeEnvironment(settings)) {
            for (Path path : env.indexPaths(index)) {
                Files.delete(path.resolve(MetadataStateFormat.STATE_DIR_NAME));
            }
        }

        // assert that we fail on shard data even without the metadata dir.
        verifyFailsOnShardData(noDataSettings, indexPath, shardDataDirName);
        verifyFailsOnShardData(noDataNoMasterSettings, indexPath, shardDataDirName);
    }

    public void testDowngradeFrom8xErrorMessage() throws IOException {
        final Settings settings = buildEnvSettings(Settings.EMPTY);
        final Environment environment = TestEnvironment.newEnvironment(settings);
        // noinspection EmptyTryBlock we're just creating the directory structure
        try (NodeEnvironment ignored = new NodeEnvironment(settings, environment)) {}
        final Path nodesPath = randomFrom(environment.dataFiles()).resolve(NODES_FOLDER);
        assertTrue(Files.isDirectory(nodesPath));
        IOUtils.rm(nodesPath);

        final BiConsumer<byte[], Matcher<String>> testCaseConsumer = (fileContent, causeMatcher) -> {
            try {
                Files.write(nodesPath, fileContent);
                final IllegalStateException e = expectThrows(IllegalStateException.class, () -> new NodeEnvironment(settings, environment));
                assertThat(
                    e.getMessage(),
                    allOf(containsString("is not compatible"), containsString("perhaps it has already been upgraded to a later version"))
                );
                assertThat(e.getCause(), instanceOf(IllegalStateException.class));
                assertThat(
                    e.getCause().getMessage().length(),
                    lessThanOrEqualTo(nodesPath.toString().length() + "[] is a file which contains [...]".length() + 256)
                );
                assertThat(e.getCause().getMessage(), causeMatcher);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };

        testCaseConsumer.accept(
            "file content which should be reported in the error message".getBytes(StandardCharsets.UTF_8),
            endsWith("is a file which contains [file content which should be reported in the error message]")
        );

        testCaseConsumer.accept(
            Stream.iterate("x", x -> x).limit(10000).collect(Collectors.joining()).getBytes(StandardCharsets.UTF_8),
            allOf(containsString("is a file which contains [xxxxxx"), endsWith("xxx...]"))
        );

        final byte[] content = randomByteArrayOfLength(1024);
        content[between(0, 255)] = (byte) 0xff; // never valid in a UTF-8 string
        testCaseConsumer.accept(content, endsWith("is a file which contains [<unreadable>]"));
    }

    private void verifyFailsOnShardData(Settings settings, Path indexPath, String shardDataDirName) {
        IllegalStateException ex = expectThrows(
            IllegalStateException.class,
            "Must fail creating NodeEnvironment on a data path that has shard data if node does not have data role",
            () -> newNodeEnvironment(settings).close()
        );

        assertThat(ex.getMessage(), containsString(indexPath.resolve(shardDataDirName).toAbsolutePath().toString()));
        assertThat(ex.getMessage(), startsWith("node does not have the data role but has shard data"));
    }

    private void verifyFailsOnMetadata(Settings settings, Path indexPath) {
        IllegalStateException ex = expectThrows(
            IllegalStateException.class,
            "Must fail creating NodeEnvironment on a data path that has index metadata if node does not have data and master roles",
            () -> newNodeEnvironment(settings).close()
        );

        assertThat(ex.getMessage(), containsString(indexPath.resolve(MetadataStateFormat.STATE_DIR_NAME).toAbsolutePath().toString()));
        assertThat(ex.getMessage(), startsWith("node does not have the data and master roles but has index metadata"));
    }

    /**
     * Converts an array of Strings to an array of Paths, adding an additional child if specified
     */
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
            .put(settings)
            .build();
    }

    public NodeEnvironment newNodeEnvironment(String[] dataPaths, Settings settings) throws IOException {
        Settings build = Settings.builder()
            .put(settings)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath().toString())
            .putList(Environment.PATH_DATA_SETTING.getKey(), dataPaths)
            .build();
        return new NodeEnvironment(build, TestEnvironment.newEnvironment(build));
    }

    public NodeEnvironment newNodeEnvironment(String[] dataPaths, String sharedDataPath, Settings settings) throws IOException {
        Settings build = Settings.builder()
            .put(settings)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath().toString())
            .put(Environment.PATH_SHARED_DATA_SETTING.getKey(), sharedDataPath)
            .putList(Environment.PATH_DATA_SETTING.getKey(), dataPaths)
            .build();
        return new NodeEnvironment(build, TestEnvironment.newEnvironment(build));
    }
}
