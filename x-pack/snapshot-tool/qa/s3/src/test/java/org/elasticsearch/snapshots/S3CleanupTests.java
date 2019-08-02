/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.snapshots;

import com.amazonaws.services.s3.internal.Constants;
import joptsimple.OptionSet;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.s3.S3RepositoryPlugin;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;

public class S3CleanupTests extends AbstractCleanupTests {

    @Override
    protected void assertBlobsByPrefix(BlobStoreRepository repository, BlobPath path, String prefix, Map<String, BlobMetaData> blobs)
            throws Exception {
        assertBusy(() -> super.assertBlobsByPrefix(repository, path, prefix, blobs), 10, TimeUnit.MINUTES);
    }

    @Override
    protected void assertCorruptionVisible(BlobStoreRepository repo, Map<String, Set<String>> indexToFiles) throws Exception {
        assertBusy(() -> super.assertCorruptionVisible(repo, indexToFiles), 10, TimeUnit.MINUTES);
    }

    @Override
    protected void assertConsistency(BlobStoreRepository repo, Executor executor) throws Exception {
        assertBusy(() -> super.assertConsistency(repo, executor), 10, TimeUnit.MINUTES);
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(S3RepositoryPlugin.class);
    }

    @Override
    protected SecureSettings credentials() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client.default.access_key", getAccessKey());
        secureSettings.setString("s3.client.default.secret_key", getSecretKey());
        return secureSettings;
    }

    @Override
    protected void createRepository(String repoName) {
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
        return System.getProperty("test.s3.endpoint", Constants.S3_HOSTNAME);
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

    @Override
    protected ThrowingRunnable commandRunnable(MockTerminal terminal, Map<String, String> nonDefaultArguments) {
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
        return () -> command.execute(terminal, options);
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
}
