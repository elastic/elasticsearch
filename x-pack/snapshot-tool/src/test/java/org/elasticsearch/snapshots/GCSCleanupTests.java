/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.snapshots;

import joptsimple.OptionSet;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.gcs.GoogleCloudStoragePlugin;

import java.util.Base64;
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
        assertThat(getBase64Credentials(), not(blankOrNullString()));

        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setFile("gcs.client.default.credentials_file", Base64.getDecoder().decode(getBase64Credentials()));
        return secureSettings;
    }

    @Override
    protected Settings nodeSettings() {
        Settings.Builder builder = Settings.builder().put(super.nodeSettings());

        if (Strings.isNullOrEmpty(getEndpoint()) == false) {
            builder.put("gcs.client.default.endpoint", getEndpoint());
        }

        if (Strings.isNullOrEmpty(getTokenUri()) == false) {
            builder.put("gcs.client.default.token_uri", getTokenUri());
        }

        return builder.build();
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

    private String getEndpoint() {
        return System.getProperty("test.google.endpoint", "");
    }

    private String getTokenUri() {
        return System.getProperty("test.google.tokenURI", "");
    }

    private String getBase64Credentials() {
        return System.getProperty("test.google.account");
    }

    @Override
    protected ThrowingRunnable commandRunnable(MockTerminal terminal, Map<String, String> nonDefaultArguments) {
        final CleanupGCSRepositoryCommand command = new CleanupGCSRepositoryCommand();
        final OptionSet options = command.getParser().parse(
                "--safety_gap_millis", nonDefaultArguments.getOrDefault("safety_gap_millis", "0"),
                "--parallelism", nonDefaultArguments.getOrDefault("parallelism", "10"),
                "--bucket", nonDefaultArguments.getOrDefault("bucket", getBucket()),
                "--base_path", nonDefaultArguments.getOrDefault("base_path", getBasePath()),
                "--base64_credentials", nonDefaultArguments.getOrDefault("base64_credentials", getBase64Credentials()),
                "--endpoint", nonDefaultArguments.getOrDefault("endpoint", getEndpoint()),
                "--token_uri", nonDefaultArguments.getOrDefault("token_uri", getTokenUri())
                );
        return () -> command.execute(terminal, options);
    }

    public void testNoCredentials() {
        expectThrows(() ->
                        executeCommand(false, Map.of("base64_credentials", "")),
                "base64_credentials option is required for cleaning up GCS repository");
    }

}
