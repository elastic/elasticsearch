/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.snapshots;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.blobstore.BlobStoreTestUtil;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Executor;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;

public abstract class AbstractCleanupTests extends ESSingleNodeTestCase {

    protected BlobStoreRepository repository;

    protected void assertBlobsByPrefix(BlobStoreRepository repository, BlobPath path, String prefix, Map<String, BlobMetaData> blobs)
            throws Exception {
        BlobStoreTestUtil.assertBlobsByPrefix(repository, path, prefix, blobs);
    }

    protected void assertConsistency(BlobStoreRepository repo, Executor executor) throws Exception {
        BlobStoreTestUtil.assertConsistency(repo, executor);
    }

    protected void assertCorruptionVisible(BlobStoreRepository repo, Map<String, Set<String>> indexToFiles) throws Exception {
       BlobStoreTestUtil.assertCorruptionVisible(repo, indexToFiles);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        createRepository("test-repo");
        repository = (BlobStoreRepository) getInstanceFromNode(RepositoriesService.class).repository("test-repo");
        cleanupRepository(repository);
    }

    private void cleanupRepository(BlobStoreRepository repository) throws Exception {
        final PlainActionFuture<Void> future = PlainActionFuture.newFuture();
        repository.threadPool().generic().execute(new ActionRunnable<>(future) {
            @Override
            protected void doRun() throws Exception {
                repository.blobStore().blobContainer(repository.basePath()).delete();
                future.onResponse(null);
            }
        });
        future.actionGet();
        assertBlobsByPrefix(repository, repository.basePath(), "", Collections.emptyMap());
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
                .put(super.nodeSettings())
                .setSecureSettings(credentials())
                .build();
    }

    protected abstract SecureSettings credentials();

    protected abstract void createRepository(String repoName);

    protected abstract ThrowingRunnable commandRunnable(MockTerminal terminal, Map<String, String> nonDefaultArguments);

    private MockTerminal executeCommand(boolean abort) throws Throwable {
        return executeCommand(abort, Collections.emptyMap());
    }

    protected MockTerminal executeCommand(boolean abort, Map<String, String> nonDefaultArguments) throws Throwable {
        final MockTerminal terminal = new MockTerminal();
        ThrowingRunnable command = commandRunnable(terminal, nonDefaultArguments);
        terminal.setVerbosity(Terminal.Verbosity.VERBOSE);
        final String input;

        if (abort) {
            input = randomValueOtherThanMany(c -> c.equalsIgnoreCase("y"), () -> randomAlphaOfLength(1));
        } else {
            input = randomBoolean() ? "y" : "Y";
        }

        terminal.addTextInput(input);

        try {
            command.run();
        } catch (ElasticsearchException e) {
            if (abort && e.getMessage().contains("Aborted by user")) {
                return terminal;
            } else {
                throw e;
            }
        } finally {
            logger.info("Cleanup command output:\n" + terminal.getOutput());
            logger.info("Cleanup command standard error:\n" + terminal.getErrorOutput());
        }

        return terminal;
    }

    protected void expectThrows(ThrowingRunnable runnable, String message) {
        ElasticsearchException ex = expectThrows(ElasticsearchException.class, runnable);
        assertThat(ex.getMessage(), containsString(message));
    }

    public void testNoBucket() {
        expectThrows(() ->
                        executeCommand(false, Map.of("bucket", "")),
                "bucket option is required for cleaning up repository");
    }

    public void testNegativeSafetyGap() {
        expectThrows(() ->
                        executeCommand(false, Map.of("safety_gap_millis", "-10")),
                "safety_gap_millis should be non-negative");
    }

    public void testInvalidParallelism() {
        expectThrows(() ->
                        executeCommand(false, Map.of("parallelism", "0")),
                "parallelism should be at least 1");
    }

    public void testBasePathTrailingSlash() {
        expectThrows(() ->
                        executeCommand(false, Map.of("base_path", "test/")),
                "there should be no trailing slash in the base path");
    }

    public void testCleanup() throws Throwable {
        logger.info("--> execute cleanup tool on empty repo, there is nothing to cleanup");
        MockTerminal terminal = executeCommand(false);
        assertThat(terminal.getOutput(), containsString("No index-N files found. Repository is empty or corrupted? Exiting"));
        createIndex("test-idx-1");
        createIndex("test-idx-2");
        createIndex("test-idx-3");
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            client().prepareIndex("test-idx-1", "doc", Integer.toString(i)).setSource("foo", "bar" + i).get();
            client().prepareIndex("test-idx-2", "doc", Integer.toString(i)).setSource("foo", "bar" + i).get();
            client().prepareIndex("test-idx-3", "doc", Integer.toString(i)).setSource("foo", "bar" + i).get();
        }
        client().admin().indices().prepareRefresh().get();

        // We run multiple iterations of snapshot -> corrupt -> cleanup -> verify -> delete snapshot
        // to make sure cleanup tool works correctly regardless of index.latest value
        for (int i = 1; i <= randomIntBetween(1, 3); i++) {
            logger.info("Iteration number {}", i);
            logger.info("--> create first snapshot");
            CreateSnapshotResponse createSnapshotResponse = client().admin()
                    .cluster()
                    .prepareCreateSnapshot("test-repo", "snap1")
                    .setWaitForCompletion(true)
                    .setIndices("test-idx-*", "-test-idx-3")
                    .get();
            assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
            assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(),
                    equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));

            assertThat(client().admin()
                            .cluster()
                            .prepareGetSnapshots("test-repo")
                            .setSnapshots("snap1")
                            .get()
                            .getSnapshots("test-repo")
                            .get(0)
                            .state(),
                    equalTo(SnapshotState.SUCCESS));


            logger.info("--> execute cleanup tool, there is nothing to cleanup");
            terminal = executeCommand(false);
            assertThat(terminal.getOutput(), containsString("Set of deletion candidates is empty. Exiting"));

            logger.info("--> check that there is no inconsistencies after running the tool");
            assertConsistency(repository, repository.threadPool().executor(ThreadPool.Names.GENERIC));

            logger.info("--> create several dangling indices");
            int numOfFiles = 0;
            long size = 0L;
            Map<String, Set<String>> indexToFiles = new TreeMap<>();
            for (int j = 0; j < randomIntBetween(1, 5); j++) {
                String name = randomValueOtherThanMany(n -> indexToFiles.containsKey(n), () -> randomAlphaOfLength(5));
                Set<String> files = new TreeSet<>();
                indexToFiles.put(name, files);
                for (int k = 0; k < randomIntBetween(1, 5); k++) {
                    String file = randomValueOtherThanMany(f -> files.contains(f), () -> randomAlphaOfLength(6));
                    files.add(file);
                    numOfFiles++;
                }
                size += BlobStoreTestUtil.createDanglingIndex(repository, name, files);
            }
            Set<String> danglingIndices = indexToFiles.keySet();

            logger.info("--> ensure dangling index folders are visible");
            assertCorruptionVisible(repository, indexToFiles);

            logger.info("--> execute cleanup tool, corruption is created latter than snapshot, there is nothing to cleanup");
            terminal = executeCommand(false);
            assertThat(terminal.getOutput(), containsString("Set of orphaned indices is empty. Exiting"));

            logger.info("--> create second snapshot");
            createSnapshotResponse = client().admin()
                    .cluster()
                    .prepareCreateSnapshot("test-repo", "snap2")
                    .setWaitForCompletion(true)
                    .setIndices("test-idx-*", "-test-idx-3")
                    .get();
            assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
            assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(),
                    equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));

            logger.info("--> execute cleanup tool again and abort");
            terminal = executeCommand(true);
            assertThat(terminal.getOutput(),
                    containsString("Set of deletion candidates has " + danglingIndices.size() + " elements: " + danglingIndices));
            assertThat(terminal.getOutput(),
                    containsString("Set of orphaned indices has " + danglingIndices.size() + " elements: " + danglingIndices));
            assertThat(terminal.getOutput(), containsString("This action is NOT REVERSIBLE"));
            for (String index : indexToFiles.keySet()) {
                assertThat(terminal.getOutput(), not(containsString("Removing orphaned index " + index)));
            }

            logger.info("--> execute cleanup tool again and confirm, dangling indices should go");
            terminal = executeCommand(false);
            assertThat(terminal.getOutput(),
                    containsString("Set of deletion candidates has " + danglingIndices.size() + " elements: " + danglingIndices));
            assertThat(terminal.getOutput(),
                    containsString("Set of orphaned indices has " + danglingIndices.size() + " elements: " + danglingIndices));
            assertThat(terminal.getOutput(), containsString("This action is NOT REVERSIBLE"));
            for (String index : indexToFiles.keySet()) {
                assertThat(terminal.getOutput(), containsString("Removing orphaned index " + index));
                for (String file : indexToFiles.get(index)) {
                    assertThat(terminal.getOutput(), containsString(index + "/" + file));
                }
            }
            assertThat(terminal.getOutput(),
                    containsString("Total files removed: " + numOfFiles));
            assertThat(terminal.getOutput(),
                    containsString("Total bytes freed: " + size));

            logger.info("--> verify that there is no inconsistencies");
            assertConsistency(repository, repository.threadPool().executor(ThreadPool.Names.GENERIC));
            logger.info("--> perform cleanup by removing snapshots");
            assertTrue(client().admin()
                    .cluster()
                    .prepareDeleteSnapshot("test-repo", "snap1")
                    .get()
                    .isAcknowledged());
            assertTrue(client().admin()
                    .cluster()
                    .prepareDeleteSnapshot("test-repo", "snap2")
                    .get()
                    .isAcknowledged());
        }
    }
}
