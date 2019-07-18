/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.snapshots;

import joptsimple.OptionSet;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.blobstore.BlobStoreTestUtil;
import org.elasticsearch.repositories.s3.S3RepositoryPlugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;

public class S3CleanupTests extends ESSingleNodeTestCase {

    private BlobStoreRepository repository;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        createRepository("test-repo");
        repository = (BlobStoreRepository) getInstanceFromNode(RepositoriesService.class).repository("test-repo");
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
                .put(super.nodeSettings())
                .setSecureSettings(credentials())
                .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(S3RepositoryPlugin.class);
    }

    private SecureSettings credentials() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client.default.access_key", getAccessKey());
        secureSettings.setString("s3.client.default.secret_key", getSecretKey());
        return secureSettings;
    }

    private void createRepository(String repoName) {
        Settings.Builder settings = Settings.builder()
                .put("bucket", getBucket())
                .put("base_path", getBasePath())
                .put("endpoint", getEndpoint());

        AcknowledgedResponse putRepositoryResponse = client().admin().cluster()
                .preparePutRepository(repoName)
                .setType("s3")
                .setSettings(settings).get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));
    }

    private String getEndpoint() {
        return System.getProperty("test.s3.endpoint");
    }

    private String getRegion() {
        return "";
    }

    private String getBucket() {
        return System.getProperty("test.s3.bucket");
    }

    private String getBasePath() {
        return System.getProperty("test.s3.base");
    }

    private String getAccessKey() {
        return System.getProperty("test.s3.account");
    }

    private String getSecretKey() {
        return System.getProperty("test.s3.key");
    }

    private MockTerminal executeCommand(boolean abort) throws Exception {
        return executeCommand(abort, Collections.emptyMap());
    }

    private MockTerminal executeCommand(boolean abort, Map<String, String> nonDefaultArguments) throws Exception {
        final CleanupS3RepositoryCommand command = new CleanupS3RepositoryCommand();
        final OptionSet options = command.getParser().parse(
                "--safety_gap_millis", nonDefaultArguments.getOrDefault("safety_gap_millis", "0"),
                "--parallelism", nonDefaultArguments.getOrDefault("parallelism", "10"),
                "--endpoint", nonDefaultArguments.getOrDefault("endpoint", getEndpoint()),
                "--region", nonDefaultArguments.getOrDefault("region", getRegion()),
                "--bucket", nonDefaultArguments.getOrDefault("bucket", getBucket()),
                "--base_path", nonDefaultArguments.getOrDefault("base_path", getBasePath()),
                "--access_key", nonDefaultArguments.getOrDefault("access_key", getAccessKey()),
                "--secret_key", nonDefaultArguments.getOrDefault("secret_key", getSecretKey()));
        final MockTerminal terminal = new MockTerminal();
        terminal.setVerbosity(Terminal.Verbosity.VERBOSE);
        final String input;

        if (abort) {
            input = randomValueOtherThanMany(c -> c.equalsIgnoreCase("y"), () -> randomAlphaOfLength(1));
        } else {
            input = randomBoolean() ? "y" : "Y";
        }

        terminal.addTextInput(input);

        try {
            command.execute(terminal, options);
        } catch (ElasticsearchException e) {
            if (abort && e.getMessage().contains("Aborted by user")) {
                return terminal;
            } else {
                throw e;
            }
        } finally {
            logger.info("Cleanup command output:\n" + terminal.getOutput());
        }

        return terminal;
    }

    private void expectThrows(ThrowingRunnable runnable, String message) {
        ElasticsearchException ex = expectThrows(ElasticsearchException.class, runnable);
        assertThat(ex.getMessage(), containsString(message));
    }

    private static Map<String, String> mapOf(String... args) {
        HashMap<String, String> map = new HashMap<>();
        for (int i = 0; i < args.length; i+=2) {
            map.put(args[i], args[i+1]);
        }
        return map;
    }

    public void testNoRegionNoEndpoint() {
        expectThrows(() ->
                        executeCommand(false, mapOf("region", "", "endpoint", "")),
                "region or endpoint option is required for cleaning up S3 repository");
    }

    public void testRegionAndEndpointSpecified() {
        expectThrows(() ->
                        executeCommand(false, mapOf("region", "test_region", "endpoint", "test_endpoint")),
                "you must not specify both region and endpoint");
    }

    public void testNoBucket() {
        expectThrows(() ->
                executeCommand(false, mapOf("bucket", "")),
                "bucket option is required for cleaning up S3 repository");
    }

    public void testNoAccessKey() {
        expectThrows(() ->
                executeCommand(false, mapOf("access_key", "")),
                "access_key option is required for cleaning up S3 repository");
    }

    public void testNoSecretKey() {
        expectThrows(() ->
                        executeCommand(false, mapOf("secret_key", "")),
                "secret_key option is required for cleaning up S3 repository");
    }

    public void testNegativeSafetyGap() {
        expectThrows(() ->
                        executeCommand(false, mapOf("safety_gap_millis", "-10")),
                "safety_gap_millis should be non-negative");
    }

    public void testInvalidParallelism() {
        expectThrows(() ->
                executeCommand(false, mapOf("parallelism", "0")),
                "parallelism should be at least 1");
    }

    public void testBasePathTrailingSlash() {
        expectThrows(() ->
                        executeCommand(false, mapOf("base_path", getBasePath() + "/")),
                "there should be not trailing slash in the base path");
    }

    public void testCleanupS3() throws Exception {
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
            BlobStoreTestUtil.assertConsistency(repository, repository.threadPool().executor(ThreadPool.Names.GENERIC));

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
            assertBusy(() -> BlobStoreTestUtil.assertCorruptionVisible(repository, indexToFiles), 10L, TimeUnit.MINUTES);

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
            assertBusy(() -> BlobStoreTestUtil.assertConsistency(repository, repository.threadPool().executor(ThreadPool.Names.GENERIC)),
                10L, TimeUnit.MINUTES);

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
