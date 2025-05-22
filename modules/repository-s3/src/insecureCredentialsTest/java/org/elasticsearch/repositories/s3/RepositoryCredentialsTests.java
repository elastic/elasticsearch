/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.s3;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.services.s3.S3Client;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.rest.AbstractRestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.admin.cluster.RestGetRepositoriesAction;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.repositories.s3.S3ClientSettings.ACCESS_KEY_SETTING;
import static org.elasticsearch.repositories.s3.S3ClientSettings.SECRET_KEY_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

@SuppressForbidden(reason = "test requires to set a System property to allow insecure settings when running in IDE")
public class RepositoryCredentialsTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(ProxyS3RepositoryPlugin.class);
    }

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    @Override
    protected Settings nodeSettings() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(ACCESS_KEY_SETTING.getConcreteSettingForNamespace("default").getKey(), "secure_default_key");
        secureSettings.setString(SECRET_KEY_SETTING.getConcreteSettingForNamespace("default").getKey(), "secure_default_secret");
        secureSettings.setString(ACCESS_KEY_SETTING.getConcreteSettingForNamespace("other").getKey(), "secure_other_key");
        secureSettings.setString(SECRET_KEY_SETTING.getConcreteSettingForNamespace("other").getKey(), "secure_other_secret");

        return Settings.builder().setSecureSettings(secureSettings).put(super.nodeSettings()).build();
    }

    public void testRepositoryCredentialsOverrideSecureCredentials() {
        final String repositoryName = "repo-creds-override";
        final Settings.Builder repositorySettings = Settings.builder()
            .put(S3Repository.BUCKET_SETTING.getKey(), "bucket")
            // repository settings for credentials override node secure settings
            .put(S3Repository.ACCESS_KEY_SETTING.getKey(), "insecure_aws_key")
            .put(S3Repository.SECRET_KEY_SETTING.getKey(), "insecure_aws_secret");

        final String clientName = randomFrom("default", "other", null);
        if (clientName != null) {
            repositorySettings.put(S3Repository.CLIENT_NAME.getKey(), clientName);
        }
        createRepository(repositoryName, repositorySettings.build());

        final RepositoriesService repositories = getInstanceFromNode(RepositoriesService.class);
        assertThat(repositories.repository(repositoryName), notNullValue());
        assertThat(repositories.repository(repositoryName), instanceOf(S3Repository.class));

        final S3Repository repository = (S3Repository) repositories.repository(repositoryName);
        try (var clientReference = repository.createBlobStore().clientReference()) {
            final ProxyS3RepositoryPlugin.ClientAndCredentials client = asInstanceOf(
                ProxyS3RepositoryPlugin.ClientAndCredentials.class,
                clientReference.client()
            );
            final AwsCredentials credentials = client.credentials.resolveCredentials();
            assertThat(credentials.accessKeyId(), is("insecure_aws_key"));
            assertThat(credentials.secretAccessKey(), is("insecure_aws_secret"));
        }

        assertCriticalWarnings(
            "[access_key] setting was deprecated in Elasticsearch and will be removed in a future release. "
                + "See the breaking changes documentation for the next major version.",
            "[secret_key] setting was deprecated in Elasticsearch and will be removed in a future release. "
                + "See the breaking changes documentation for the next major version.",
            S3Repository.INSECURE_CREDENTIALS_DEPRECATION_WARNING
        );
    }

    public void testReinitSecureCredentials() {
        final String clientName = randomFrom("default", "other");

        final Settings.Builder repositorySettings = Settings.builder().put(S3Repository.BUCKET_SETTING.getKey(), "bucket");
        final boolean hasInsecureSettings = randomBoolean();
        if (hasInsecureSettings) {
            // repository settings for credentials override node secure settings
            repositorySettings.put(S3Repository.ACCESS_KEY_SETTING.getKey(), "insecure_aws_key");
            repositorySettings.put(S3Repository.SECRET_KEY_SETTING.getKey(), "insecure_aws_secret");
        } else {
            repositorySettings.put(S3Repository.CLIENT_NAME.getKey(), clientName);
        }

        final String repositoryName = "repo-reinit-creds";
        createRepository(repositoryName, repositorySettings.build());

        final RepositoriesService repositories = getInstanceFromNode(RepositoriesService.class);
        assertThat(repositories.repository(repositoryName), notNullValue());
        assertThat(repositories.repository(repositoryName), instanceOf(S3Repository.class));

        final S3Repository repository = (S3Repository) repositories.repository(repositoryName);
        try (AmazonS3Reference clientReference = ((S3BlobStore) repository.blobStore()).clientReference()) {
            final S3Client client = clientReference.client();
            assertThat(client, instanceOf(ProxyS3RepositoryPlugin.ClientAndCredentials.class));

            final AwsCredentials credentials = ((ProxyS3RepositoryPlugin.ClientAndCredentials) client).credentials.resolveCredentials();
            if (hasInsecureSettings) {
                assertThat(credentials.accessKeyId(), is("insecure_aws_key"));
                assertThat(credentials.secretAccessKey(), is("insecure_aws_secret"));
            } else if ("other".equals(clientName)) {
                assertThat(credentials.accessKeyId(), is("secure_other_key"));
                assertThat(credentials.secretAccessKey(), is("secure_other_secret"));
            } else {
                assertThat(credentials.accessKeyId(), is("secure_default_key"));
                assertThat(credentials.secretAccessKey(), is("secure_default_secret"));
            }

            // new settings
            final MockSecureSettings newSecureSettings = new MockSecureSettings();
            newSecureSettings.setString("s3.client." + clientName + ".access_key", "new_secret_aws_key");
            newSecureSettings.setString("s3.client." + clientName + ".secret_key", "new_secret_aws_secret");
            final Settings newSettings = Settings.builder()
                .put(S3Repository.BUCKET_SETTING.getKey(), "bucket")
                .setSecureSettings(newSecureSettings)
                .build();
            // reload S3 plugin settings
            final PluginsService plugins = getInstanceFromNode(PluginsService.class);
            final ProxyS3RepositoryPlugin plugin = plugins.filterPlugins(ProxyS3RepositoryPlugin.class).findFirst().get();
            plugin.reload(newSettings);

            // check the not-yet-closed client reference still has the same credentials
            if (hasInsecureSettings) {
                assertThat(credentials.accessKeyId(), is("insecure_aws_key"));
                assertThat(credentials.secretAccessKey(), is("insecure_aws_secret"));
            } else if ("other".equals(clientName)) {
                assertThat(credentials.accessKeyId(), is("secure_other_key"));
                assertThat(credentials.secretAccessKey(), is("secure_other_secret"));
            } else {
                assertThat(credentials.accessKeyId(), is("secure_default_key"));
                assertThat(credentials.secretAccessKey(), is("secure_default_secret"));
            }
        }

        // check credentials have been updated
        try (AmazonS3Reference clientReference = ((S3BlobStore) repository.blobStore()).clientReference()) {
            final S3Client client = clientReference.client();
            assertThat(client, instanceOf(ProxyS3RepositoryPlugin.ClientAndCredentials.class));

            final AwsCredentials newCredentials = ((ProxyS3RepositoryPlugin.ClientAndCredentials) client).credentials.resolveCredentials();
            if (hasInsecureSettings) {
                assertThat(newCredentials.accessKeyId(), is("insecure_aws_key"));
                assertThat(newCredentials.secretAccessKey(), is("insecure_aws_secret"));
            } else {
                assertThat(newCredentials.accessKeyId(), is("new_secret_aws_key"));
                assertThat(newCredentials.secretAccessKey(), is("new_secret_aws_secret"));
            }
        }

        if (hasInsecureSettings) {
            assertCriticalWarnings(
                "[access_key] setting was deprecated in Elasticsearch and will be removed in a future release. "
                    + "See the breaking changes documentation for the next major version.",
                "[secret_key] setting was deprecated in Elasticsearch and will be removed in a future release. "
                    + "See the breaking changes documentation for the next major version.",
                S3Repository.INSECURE_CREDENTIALS_DEPRECATION_WARNING
            );
        }
    }

    public void testInsecureRepositoryCredentials() throws Exception {
        final String repositoryName = "repo-insecure-creds";
        createRepository(
            repositoryName,
            Settings.builder()
                .put(S3Repository.BUCKET_SETTING.getKey(), "bucket")
                .put(S3Repository.ACCESS_KEY_SETTING.getKey(), "insecure_aws_key")
                .put(S3Repository.SECRET_KEY_SETTING.getKey(), "insecure_aws_secret")
                .build()
        );

        final RestRequest fakeRestRequest = new FakeRestRequest();
        fakeRestRequest.params().put("repository", repositoryName);
        final RestGetRepositoriesAction action = new RestGetRepositoriesAction(getInstanceFromNode(SettingsFilter.class));

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<AssertionError> error = new AtomicReference<>();
        action.handleRequest(fakeRestRequest, new AbstractRestChannel(fakeRestRequest, true) {
            @Override
            public void sendResponse(RestResponse response) {
                try {
                    String responseAsString = response.content().utf8ToString();
                    assertThat(responseAsString, containsString(repositoryName));
                    assertThat(responseAsString, not(containsString("insecure_")));
                } catch (final AssertionError ex) {
                    error.set(ex);
                }
                latch.countDown();
            }
        }, getInstanceFromNode(NodeClient.class));

        latch.await();
        if (error.get() != null) {
            throw error.get();
        }

        assertWarnings(S3Repository.INSECURE_CREDENTIALS_DEPRECATION_WARNING);
    }

    private void createRepository(final String name, final Settings repositorySettings) {
        assertAcked(
            clusterAdmin().preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, name)
                .setType(S3Repository.TYPE)
                .setVerify(false)
                .setSettings(repositorySettings)
        );
    }

    /**
     * A S3 repository plugin that keeps track of the credentials used to build an AmazonS3 client
     */
    public static final class ProxyS3RepositoryPlugin extends S3RepositoryPlugin {

        public ProxyS3RepositoryPlugin(Settings settings) {
            super(settings);
        }

        @Override
        S3Service s3Service(Environment environment, Settings nodeSettings, ResourceWatcherService resourceWatcherService) {
            return new ProxyS3Service(environment, nodeSettings, resourceWatcherService);
        }

        /**
         * This wrapper exposes a copy of the AWS credentials that the S3Client uses.
         */
        public static final class ClientAndCredentials extends AmazonS3Wrapper {
            final AwsCredentialsProvider credentials;
            // The httpClient must be explicitly closed. Closure of the S3Client, which uses the httpClient, will not do so.
            private final SdkHttpClient httpClient;

            ClientAndCredentials(S3Client delegate, SdkHttpClient httpClient, AwsCredentialsProvider credentials) {
                super(delegate);
                this.httpClient = httpClient;
                this.credentials = credentials;
            }

            @Override
            public String serviceName() {
                return "ClientAndCredentials";
            }

            @Override
            public void close() {
                super.close();
                httpClient.close();
            }
        }

        /**
         * A {@link S3Service} wrapper that supports access to a copy of the credentials given to the S3Client.
         */
        public static final class ProxyS3Service extends S3Service {

            private static final Logger logger = LogManager.getLogger(ProxyS3Service.class);

            ProxyS3Service(Environment environment, Settings nodeSettings, ResourceWatcherService resourceWatcherService) {
                super(environment, nodeSettings, resourceWatcherService, () -> null);
            }

            @Override
            S3Client buildClient(final S3ClientSettings clientSettings, SdkHttpClient httpClient) {
                final S3Client client = super.buildClient(clientSettings, httpClient);
                return new ClientAndCredentials(
                    client,
                    httpClient,
                    buildCredentials(logger, clientSettings, webIdentityTokenCredentialsProvider)
                );
            }

        }
    }
}
