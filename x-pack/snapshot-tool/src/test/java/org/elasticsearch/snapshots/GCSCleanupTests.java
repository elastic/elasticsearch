package org.elasticsearch.snapshots;

import joptsimple.OptionSet;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.gcs.GoogleCloudStoragePlugin;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Map;

import static org.hamcrest.Matchers.blankOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class GCSCleanupTests extends AbstractCleanupTests {
    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(GoogleCloudStoragePlugin.class);
    }

    @Override
    protected SecureSettings credentials() {
        assertThat(getBucket(), not(blankOrNullString()));
        assertThat(getCredentialsFiles(), not(blankOrNullString()));

        MockSecureSettings secureSettings = new MockSecureSettings();
        try {
            secureSettings.setFile("gcs.client.default.credentials_file", Files.readAllBytes(Paths.get(getCredentialsFiles())));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return secureSettings;
    }

    @Override
    protected void createRepository(final String repoName) {
        AcknowledgedResponse putRepositoryResponse = client().admin().cluster().preparePutRepository("test-repo")
                .setType("gcs")
                .setSettings(Settings.builder()
                        .put("bucket", getBucket())
                        .put("base_path", getBasePath())
                ).get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));
    }

    private String getBucket() {
        return System.getProperty("test.google.bucket");
    }

    private String getBasePath() {
        return System.getProperty("test.google.base");
    }

    private String getCredentialsFiles() {
        return System.getProperty("test.google.credentials_file");
    }

    @Override
    protected ThrowingRunnable commandRunnable(MockTerminal terminal, Map<String, String> nonDefaultArguments) {
        final CleanupGCSRepositoryCommand command = new CleanupGCSRepositoryCommand();
        final OptionSet options = command.getParser().parse(
                "--safety_gap_millis", nonDefaultArguments.getOrDefault("safety_gap_millis", "0"),
                "--parallelism", nonDefaultArguments.getOrDefault("parallelism", "10"),
                "--bucket", nonDefaultArguments.getOrDefault("bucket", getBucket()),
                "--base_path", nonDefaultArguments.getOrDefault("base_path", getBasePath()),
                "--credentials_file", nonDefaultArguments.getOrDefault("credentials_file", getCredentialsFiles()));
        return () -> command.execute(terminal, options);
    }

    public void testNoCredentialsFile() {
        expectThrows(() ->
                        executeCommand(false, Map.of("credentials_file", "")),
                "credentials_file option is required for cleaning up GCS repository");
    }

}
