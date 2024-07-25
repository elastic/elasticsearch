/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.env;

import org.apache.logging.log4j.Level;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.gateway.MetadataStateFormat;
import org.elasticsearch.gateway.PersistedClusterStateService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.NodeRoles;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.hamcrest.Matchers;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystemException;
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

import static org.elasticsearch.env.NodeEnvironment.checkForIndexCompatibility;
import static org.elasticsearch.env.NodeMetadata.NODE_VERSION_KEY;
import static org.elasticsearch.env.NodeMetadata.OLDEST_INDEX_VERSION_KEY;
import static org.elasticsearch.gateway.PersistedClusterStateService.METADATA_DIRECTORY_NAME;
import static org.elasticsearch.test.NodeRoles.nonDataNode;
import static org.elasticsearch.test.NodeRoles.nonMasterNode;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.startsWith;

@LuceneTestCase.SuppressFileSystems("ExtrasFS") // TODO: fix test to allow extras
public class NodeEnvironmentTests extends ESTestCase {
    private final IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("foo", Settings.EMPTY);

    public void testNodeLock() throws IOException {
        final Settings settings = buildEnvSettings(Settings.EMPTY);
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

    // using a literal string here because the logger is mentioned in the docs, and therefore must only be changed with care
    private static final String NODE_ENVIRONMENT_LOGGER_NAME = "org.elasticsearch.env.NodeEnvironment";

    @TestLogging(reason = "test includes assertions about DEBUG logging", value = NODE_ENVIRONMENT_LOGGER_NAME + ":DEBUG")
    public void testShardLock() throws Exception {
        try (var env = newNodeEnvironment()) {

            Index index = new Index("foo", "fooUUID");

            try (var mockLog = MockLog.capture(NodeEnvironment.class); var lock = env.shardLock(new ShardId(index, 0), "1")) {
                mockLog.addExpectation(
                    new MockLog.SeenEventExpectation(
                        "hot threads logging",
                        NODE_ENVIRONMENT_LOGGER_NAME,
                        Level.DEBUG,
                        "hot threads while failing to obtain shard lock for [foo][0]: obtaining shard lock for [2] timed out after *"
                    )
                );
                mockLog.addExpectation(
                    new MockLog.UnseenEventExpectation(
                        "second attempt should be suppressed due to throttling",
                        NODE_ENVIRONMENT_LOGGER_NAME,
                        Level.DEBUG,
                        "hot threads while failing to obtain shard lock for [foo][0]: obtaining shard lock for [3] timed out after *"
                    )
                );

                assertEquals(new ShardId(index, 0), lock.getShardId());

                expectThrows(ShardLockObtainFailedException.class, () -> env.shardLock(new ShardId(index, 0), "2"));

                for (Path path : env.indexPaths(index)) {
                    Files.createDirectories(path.resolve("0"));
                    Files.createDirectories(path.resolve("1"));
                }
                expectThrows(
                    ShardLockObtainFailedException.class,
                    () -> env.lockAllForIndex(index, idxSettings, "3", randomIntBetween(0, 10))
                );

                mockLog.assertAllExpectationsMatched();
            }

            // can lock again?
            env.shardLock(new ShardId(index, 0), "4").close();

            List<ShardLock> locks = new ArrayList<>();
            try {
                locks.addAll(env.lockAllForIndex(index, idxSettings, "5", randomIntBetween(0, 10)));
                expectThrows(ShardLockObtainFailedException.class, () -> env.shardLock(new ShardId(index, 0), "6"));
            } finally {
                IOUtils.close(locks);
            }

            assertTrue("LockedShards: " + env.lockedShards(), env.lockedShards().isEmpty());
        }
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
            () -> env.deleteShardDirectorySafe(new ShardId(index, 0), idxSettings, shardPaths -> {
                assert false : "should not be called " + shardPaths;
            })
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
            () -> env.deleteIndexDirectorySafe(index, randomIntBetween(0, 10), idxSettings, indexPaths -> {
                assert false : "should not be called " + indexPaths;
            })
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

        final int threads = randomIntBetween(2, 5);
        final int iters = scaledRandomIntBetween(10000, 100000);
        startInParallel(threads, tid -> {
            for (int i = 0; i < iters; i++) {
                int shard = randomIntBetween(0, counts.length - 1);
                try {
                    try (ShardLock autoCloses = env.shardLock(new ShardId("foo", "fooUUID", shard), "1", scaledRandomIntBetween(0, 10))) {
                        counts[shard].value++;
                        countsAtomic[shard].incrementAndGet();
                        assertEquals(flipFlop[shard].incrementAndGet(), 1);
                        assertEquals(flipFlop[shard].decrementAndGet(), 0);
                    }
                } catch (ShardLockObtainFailedException ex) {
                    // ok
                }
            }
        });
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
            equalTo(stringsToPaths(dataPaths, "indices/" + index.getUUID() + "/0"))
        );

        assertThat(
            "index paths uses the regular template",
            env.indexPaths(index),
            equalTo(stringsToPaths(dataPaths, "indices/" + index.getUUID()))
        );

        assertThat(env.availableShardPaths(sid), equalTo(env.availableShardPaths(sid)));
        assertThat(
            env.resolveCustomLocation("/tmp/foo", sid).toAbsolutePath(),
            equalTo(PathUtils.get("/tmp/foo/0/" + index.getUUID() + "/0").toAbsolutePath())
        );

        assertThat(
            "shard paths with a custom data_path should contain only regular paths",
            env.availableShardPaths(sid),
            equalTo(stringsToPaths(dataPaths, "indices/" + index.getUUID() + "/0"))
        );

        assertThat(
            "index paths uses the regular template",
            env.indexPaths(index),
            equalTo(stringsToPaths(dataPaths, "indices/" + index.getUUID()))
        );

        env.close();
    }

    public void testExistingTempFiles() throws IOException {
        String[] paths = tmpPaths();
        // simulate some previous left over temp files
        for (String path : randomSubsetOf(randomIntBetween(1, paths.length), paths)) {
            final Path dataPath = PathUtils.get(path);
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
            final Path dataPath = PathUtils.get(path);
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
            .put(NodeRoles.removeRoles(nonDataNode(settings), Set.of(DiscoveryNodeRole.MASTER_ROLE)))
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

    public void testBlocksDowngradeToVersionWithMultipleNodesInDataPath() throws IOException {
        final Settings settings = buildEnvSettings(Settings.EMPTY);
        for (int i = 0; i < 2; i++) { // ensure the file gets created again if missing
            try (NodeEnvironment env = newNodeEnvironment(settings)) {
                for (Path dataPath : env.nodeDataPaths()) {
                    final Path nodesPath = dataPath.resolve("nodes");
                    assertTrue(Files.isRegularFile(nodesPath));
                    assertThat(
                        Files.readString(nodesPath, StandardCharsets.UTF_8),
                        allOf(
                            containsString("written by Elasticsearch"),
                            containsString("prevent a downgrade"),
                            containsString("data loss")
                        )
                    );
                    Files.delete(nodesPath);
                }
            }
        }
    }

    public void testIndexCompatibilityChecks() throws IOException {
        final Settings settings = buildEnvSettings(Settings.EMPTY);

        try (NodeEnvironment env = newNodeEnvironment(settings)) {
            try (
                PersistedClusterStateService.Writer writer = new PersistedClusterStateService(
                    env.nodeDataPaths(),
                    env.nodeId(),
                    xContentRegistry(),
                    new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                    () -> 0L
                ).createWriter()
            ) {
                writer.writeFullStateAndCommit(
                    1L,
                    ClusterState.builder(ClusterName.DEFAULT)
                        .metadata(
                            Metadata.builder()
                                .persistentSettings(Settings.builder().put(Metadata.SETTING_READ_ONLY_SETTING.getKey(), true).build())
                                .build()
                        )
                        .build()
                );
            }

            Version oldVersion = Version.fromId(between(1, Version.CURRENT.minimumCompatibilityVersion().id - 1));
            IndexVersion oldIndexVersion = IndexVersion.fromId(between(1, IndexVersions.MINIMUM_COMPATIBLE.id() - 1));
            Version previousNodeVersion = Version.fromId(between(Version.CURRENT.minimumCompatibilityVersion().id, Version.CURRENT.id - 1));
            overrideOldestIndexVersion(oldIndexVersion, previousNodeVersion, env.nodeDataPaths());

            IllegalStateException ex = expectThrows(
                IllegalStateException.class,
                "Must fail the check on index that's too old",
                () -> checkForIndexCompatibility(logger, env.dataPaths())
            );

            assertThat(
                ex.getMessage(),
                allOf(
                    containsString("Cannot start this node"),
                    containsString("it holds metadata for indices with version [" + oldIndexVersion.toReleaseVersion() + "]"),
                    containsString(
                        "Revert this node to version ["
                            + (previousNodeVersion.major == Version.V_8_0_0.major ? Version.V_7_17_0 : previousNodeVersion)
                            + "]"
                    )
                )
            );

            // This should work
            overrideOldestIndexVersion(IndexVersions.MINIMUM_COMPATIBLE, previousNodeVersion, env.nodeDataPaths());
            checkForIndexCompatibility(logger, env.dataPaths());

            // Trying to boot with newer version should pass this check
            overrideOldestIndexVersion(NodeMetadataTests.tooNewIndexVersion(), previousNodeVersion, env.nodeDataPaths());
            checkForIndexCompatibility(logger, env.dataPaths());

            // Simulate empty old index version, attempting to upgrade before 7.17
            removeOldestIndexVersion(oldVersion, env.nodeDataPaths());

            ex = expectThrows(
                IllegalStateException.class,
                "Must fail the check on index that's too old",
                () -> checkForIndexCompatibility(logger, env.dataPaths())
            );

            assertThat(
                ex.getMessage(),
                allOf(
                    startsWith("cannot upgrade a node from version [" + oldVersion + "] directly"),
                    containsString("upgrade to version [" + Build.current().minWireCompatVersion())
                )
            );
        }
    }

    public void testSymlinkDataDirectory() throws Exception {
        Path tempDir = createTempDir().toAbsolutePath();
        Path dataPath = tempDir.resolve("data");
        Files.createDirectories(dataPath);
        Path symLinkPath = tempDir.resolve("data_symlink");
        try {
            Files.createSymbolicLink(symLinkPath, dataPath);
        } catch (FileSystemException e) {
            if (IOUtils.WINDOWS && "A required privilege is not held by the client".equals(e.getReason())) {
                throw new AssumptionViolatedException("Symlinks on Windows need admin privileges", e);
            } else {
                throw e;
            }
        }

        NodeEnvironment env = newNodeEnvironment(new String[] { symLinkPath.toString() }, "/tmp", Settings.EMPTY);

        assertTrue(Files.exists(symLinkPath));
        env.close();
    }

    public void testGetBestDowngradeVersion() {
        assertThat(NodeEnvironment.getBestDowngradeVersion("7.17.0"), Matchers.equalTo("7.17.0"));
        assertThat(NodeEnvironment.getBestDowngradeVersion("7.17.5"), Matchers.equalTo("7.17.5"));
        assertThat(NodeEnvironment.getBestDowngradeVersion("7.17.1234"), Matchers.equalTo("7.17.1234"));
        assertThat(NodeEnvironment.getBestDowngradeVersion("7.18.0"), Matchers.equalTo("7.18.0"));
        assertThat(NodeEnvironment.getBestDowngradeVersion("7.17.x"), Matchers.equalTo("7.17.0"));
        assertThat(NodeEnvironment.getBestDowngradeVersion("7.17.5-SNAPSHOT"), Matchers.equalTo("7.17.0"));
        assertThat(NodeEnvironment.getBestDowngradeVersion("7.17.6b"), Matchers.equalTo("7.17.0"));
        assertThat(NodeEnvironment.getBestDowngradeVersion("7.16.0"), Matchers.equalTo("7.17.0"));
        // when we get to version 7.2147483648.0 we will have to rethink our approach, but for now we return 7.17.0 with an integer overflow
        assertThat(NodeEnvironment.getBestDowngradeVersion("7." + Integer.MAX_VALUE + "0.0"), Matchers.equalTo("7.17.0"));
        assertThat(NodeEnvironment.getBestDowngradeVersion("foo"), Matchers.equalTo("7.17.0"));
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

    private static void overrideOldestIndexVersion(IndexVersion oldestIndexVersion, Version previousNodeVersion, Path... dataPaths)
        throws IOException {
        for (final Path dataPath : dataPaths) {
            final Path indexPath = dataPath.resolve(METADATA_DIRECTORY_NAME);
            if (Files.exists(indexPath)) {
                try (DirectoryReader reader = DirectoryReader.open(new NIOFSDirectory(dataPath.resolve(METADATA_DIRECTORY_NAME)))) {
                    final Map<String, String> userData = reader.getIndexCommit().getUserData();
                    final IndexWriterConfig indexWriterConfig = new IndexWriterConfig(new KeywordAnalyzer());

                    try (
                        IndexWriter indexWriter = new IndexWriter(
                            new NIOFSDirectory(dataPath.resolve(METADATA_DIRECTORY_NAME)),
                            indexWriterConfig
                        )
                    ) {
                        final Map<String, String> commitData = new HashMap<>(userData);
                        commitData.put(NODE_VERSION_KEY, Integer.toString(previousNodeVersion.id));
                        commitData.put(OLDEST_INDEX_VERSION_KEY, Integer.toString(oldestIndexVersion.id()));
                        indexWriter.setLiveCommitData(commitData.entrySet());
                        indexWriter.commit();
                    }
                }
            }
        }
    }

    private static void removeOldestIndexVersion(Version oldVersion, Path... dataPaths) throws IOException {
        for (final Path dataPath : dataPaths) {
            final Path indexPath = dataPath.resolve(METADATA_DIRECTORY_NAME);
            if (Files.exists(indexPath)) {
                try (DirectoryReader reader = DirectoryReader.open(new NIOFSDirectory(dataPath.resolve(METADATA_DIRECTORY_NAME)))) {
                    final Map<String, String> userData = reader.getIndexCommit().getUserData();
                    final IndexWriterConfig indexWriterConfig = new IndexWriterConfig(new KeywordAnalyzer());

                    try (
                        IndexWriter indexWriter = new IndexWriter(
                            new NIOFSDirectory(dataPath.resolve(METADATA_DIRECTORY_NAME)),
                            indexWriterConfig
                        )
                    ) {
                        final Map<String, String> commitData = new HashMap<>(userData);
                        commitData.put(NODE_VERSION_KEY, Integer.toString(oldVersion.id));
                        commitData.remove(OLDEST_INDEX_VERSION_KEY);
                        indexWriter.setLiveCommitData(commitData.entrySet());
                        indexWriter.commit();
                    }
                }
            }
        }
    }
}
