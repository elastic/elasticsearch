package org.elasticsearch.snapshots;

import joptsimple.OptionSet;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.s3.S3RepositoryPlugin;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.Collection;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

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


    public void testSnapshot() throws Exception {
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

       final String snapshotName = "test-snap-" + System.currentTimeMillis();

       logger.info("--> snapshot");
       CreateSnapshotResponse createSnapshotResponse = client().admin()
               .cluster()
               .prepareCreateSnapshot("test-repo", snapshotName)
               .setWaitForCompletion(true)
               .setIndices("test-idx-*", "-test-idx-3")
               .get();
       assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
       assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(),
               equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));

       assertThat(client().admin()
                       .cluster()
                       .prepareGetSnapshots("test-repo")
                       .setSnapshots(snapshotName)
                       .get()
                       .getSnapshots()
                       .get(0)
                       .state(),
               equalTo(SnapshotState.SUCCESS));

       final Environment environment = TestEnvironment.newEnvironment(node().settings());
       final CleanupS3RepositoryCommand command = new CleanupS3RepositoryCommand();
       final MockTerminal terminal = new MockTerminal();
       final OptionSet options = command.getParser().parse(
               "--endpoint", getEndpoint(),
               "--bucket", getBucket(),
               "--basePath", getBasePath(),
               "--access_key", getAccessKey(),
               "--secret_key", getSecretKey());

       command.execute(terminal, options, environment);

       assertTrue(client().admin()
               .cluster()
               .prepareDeleteSnapshot("test-repo", snapshotName)
               .get()
               .isAcknowledged());
   }
}
