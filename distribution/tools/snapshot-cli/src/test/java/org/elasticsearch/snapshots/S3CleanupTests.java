package org.elasticsearch.snapshots;

import joptsimple.OptionSet;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.blobstore.BlobStoreTestUtil;
import org.elasticsearch.repositories.s3.S3RepositoryPlugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.ByteArrayInputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;

public class S3CleanupTests extends ESSingleNodeTestCase {

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

    private MockTerminal executeCommand(boolean abort, Map<String, String> nonDefaultArguments)
            throws Exception {
        final Environment environment = TestEnvironment.newEnvironment(node().settings());
        final CleanupS3RepositoryCommand command = new CleanupS3RepositoryCommand();
        final OptionSet options = command.getParser().parse(
                "--safety_gap_millis", nonDefaultArguments.getOrDefault("safety_gap_millis", "0"),
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
            command.execute(terminal, options, environment);
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

    public void testNoRegionNoEndpoint() {
        expectThrows(() ->
                        executeCommand(false, Map.of("region", "", "endpoint", "")),
                "region or endpoint option is required for cleaning up S3 repository");
    }

    public void testRegionAndEndpointSpecified() {
        expectThrows(() ->
                        executeCommand(false, Map.of("region", "test_region", "endpoint", "test_endpoint")),
                "you must not specify both region and endpoint");
    }

    public void testNoBucket()  {
        expectThrows(() ->
                executeCommand(false, Map.of("bucket", "")),
                "bucket option is required for cleaning up S3 repository");
    }

    public void testNoAccessKey() {
        expectThrows(() ->
                executeCommand(false, Map.of("access_key", "")),
                "access_key option is required for cleaning up S3 repository");
    }

    public void testNoSecretKey() {
        expectThrows(() ->
                        executeCommand(false, Map.of("secret_key", "")),
                "secret_key option is required for cleaning up S3 repository");
    }

    public void testNegativeSafetyGap() {
        expectThrows(() ->
                        executeCommand(false, Map.of("safety_gap_millis", "-10")),
                "safety_gap_millis should be non-negative");
    }

    public void testCleanupS3() throws Exception {
       createRepository("test-repo");
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
       MockTerminal terminal = executeCommand(false);
       assertThat(terminal.getOutput(), containsString("Set of deletion candidates is empty. Exiting"));

       logger.info("--> check that there is no inconsistencies after running the tool");
       final BlobStoreRepository repo =
                (BlobStoreRepository) getInstanceFromNode(RepositoriesService.class).repository("test-repo");
       final Executor genericExec = repo.threadPool().executor(ThreadPool.Names.GENERIC);
       BlobStoreTestUtil.assertConsistency(repo, genericExec);

       logger.info("--> create dangling index folder indices/foo");
       final PlainActionFuture<Void> future = PlainActionFuture.newFuture();
       genericExec.execute(new ActionRunnable<>(future) {
            @Override
            protected void doRun() throws Exception {
                final BlobStore blobStore = repo.blobStore();
                blobStore.blobContainer(BlobPath.cleanPath().add(getBasePath()).add("indices").add("foo"))
                        .writeBlob("bar", new ByteArrayInputStream(new byte[0]), 0, false);
                future.onResponse(null);
            }
        });

       logger.info("--> ensure dangling index folder is visible");
       assertBusy(() -> assertCorruptionVisible(repo, genericExec), 10, TimeUnit.MINUTES);

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
       assertThat(terminal.getOutput(), containsString("Set of deletion candidates is [foo]"));
       assertThat(terminal.getOutput(), containsString("Set of leaked indices is [foo]"));
       assertThat(terminal.getOutput(), containsString("This action is NOT REVERSIBLE"));
       assertThat(terminal.getOutput(), not(containsString("Removing leaked index foo")));

       logger.info("--> execute cleanup tool again and confirm, indices/foo should go");
       terminal = executeCommand(false);
       assertThat(terminal.getOutput(), containsString("Set of deletion candidates is [foo]"));
       assertThat(terminal.getOutput(), containsString("Set of leaked indices is [foo]"));
       assertThat(terminal.getOutput(), containsString("This action is NOT REVERSIBLE"));
       assertThat(terminal.getOutput(), containsString("Removing leaked index foo"));
       assertThat(terminal.getOutput(), containsString("foo/bar"));

       logger.info("--> verify that there is no inconsistencies");
       BlobStoreTestUtil.assertConsistency(repo, genericExec);

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


    protected boolean assertCorruptionVisible(BlobStoreRepository repo, Executor executor) throws Exception {
        final PlainActionFuture<Boolean> future = PlainActionFuture.newFuture();
        executor.execute(new ActionRunnable<>(future) {
            @Override
            protected void doRun() throws Exception {
                final BlobStore blobStore = repo.blobStore();
                future.onResponse(
                        blobStore.blobContainer(BlobPath.cleanPath().add(getBasePath()).add("indices")).children().containsKey("foo")
                                && blobStore.blobContainer(BlobPath.cleanPath().add(getBasePath()).add("indices").add("foo")).blobExists(
                                        "bar")
                );
            }
        });
        return future.actionGet();
    }
}
