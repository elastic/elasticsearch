/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.s3;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.rest.AbstractRestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.admin.cluster.RestGetRepositoriesAction;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.security.AccessController;
import java.security.PrivilegedAction;
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

    static {
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            // required for client settings overwriting when running in IDE
            System.setProperty("es.allow_insecure_settings", "true");
            return null;
        });
    }

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
        final AmazonS3 client = repository.createBlobStore().clientReference().client();
        assertThat(client, instanceOf(ProxyS3RepositoryPlugin.ClientAndCredentials.class));

        final AWSCredentials credentials = ((ProxyS3RepositoryPlugin.ClientAndCredentials) client).credentials.getCredentials();
        assertThat(credentials.getAWSAccessKeyId(), is("insecure_aws_key"));
        assertThat(credentials.getAWSSecretKey(), is("insecure_aws_secret"));

        assertCriticalWarnings(
            "[secret_key] setting was deprecated in Elasticsearch and will be removed in a future release.",
            "Using s3 access/secret key from repository settings. Instead store these in named clients and"
                + " the elasticsearch keystore for secure settings.",
            "[access_key] setting was deprecated in Elasticsearch and will be removed in a future release."
        );
    }

    public void testReinitSecureCredentials() {
        final String clientName = randomFrom("default", "other");

        final Settings.Builder repositorySettings = Settings.builder();
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
            final AmazonS3 client = clientReference.client();
            assertThat(client, instanceOf(ProxyS3RepositoryPlugin.ClientAndCredentials.class));

            final AWSCredentials credentials = ((ProxyS3RepositoryPlugin.ClientAndCredentials) client).credentials.getCredentials();
            if (hasInsecureSettings) {
                assertThat(credentials.getAWSAccessKeyId(), is("insecure_aws_key"));
                assertThat(credentials.getAWSSecretKey(), is("insecure_aws_secret"));
            } else if ("other".equals(clientName)) {
                assertThat(credentials.getAWSAccessKeyId(), is("secure_other_key"));
                assertThat(credentials.getAWSSecretKey(), is("secure_other_secret"));
            } else {
                assertThat(credentials.getAWSAccessKeyId(), is("secure_default_key"));
                assertThat(credentials.getAWSSecretKey(), is("secure_default_secret"));
            }

            // new settings
            final MockSecureSettings newSecureSettings = new MockSecureSettings();
            newSecureSettings.setString("s3.client." + clientName + ".access_key", "new_secret_aws_key");
            newSecureSettings.setString("s3.client." + clientName + ".secret_key", "new_secret_aws_secret");
            final Settings newSettings = Settings.builder().setSecureSettings(newSecureSettings).build();
            // reload S3 plugin settings
            final PluginsService plugins = getInstanceFromNode(PluginsService.class);
            final ProxyS3RepositoryPlugin plugin = plugins.filterPlugins(ProxyS3RepositoryPlugin.class).get(0);
            plugin.reload(newSettings);

            // check the not-yet-closed client reference still has the same credentials
            if (hasInsecureSettings) {
                assertThat(credentials.getAWSAccessKeyId(), is("insecure_aws_key"));
                assertThat(credentials.getAWSSecretKey(), is("insecure_aws_secret"));
            } else if ("other".equals(clientName)) {
                assertThat(credentials.getAWSAccessKeyId(), is("secure_other_key"));
                assertThat(credentials.getAWSSecretKey(), is("secure_other_secret"));
            } else {
                assertThat(credentials.getAWSAccessKeyId(), is("secure_default_key"));
                assertThat(credentials.getAWSSecretKey(), is("secure_default_secret"));
            }
        }

        // check credentials have been updated
        try (AmazonS3Reference clientReference = ((S3BlobStore) repository.blobStore()).clientReference()) {
            final AmazonS3 client = clientReference.client();
            assertThat(client, instanceOf(ProxyS3RepositoryPlugin.ClientAndCredentials.class));

            final AWSCredentials newCredentials = ((ProxyS3RepositoryPlugin.ClientAndCredentials) client).credentials.getCredentials();
            if (hasInsecureSettings) {
                assertThat(newCredentials.getAWSAccessKeyId(), is("insecure_aws_key"));
                assertThat(newCredentials.getAWSSecretKey(), is("insecure_aws_secret"));
            } else {
                assertThat(newCredentials.getAWSAccessKeyId(), is("new_secret_aws_key"));
                assertThat(newCredentials.getAWSSecretKey(), is("new_secret_aws_secret"));
            }
        }

        if (hasInsecureSettings) {
            assertCriticalWarnings(
                "[secret_key] setting was deprecated in Elasticsearch and will be removed in a future release.",
                "Using s3 access/secret key from repository settings. Instead store these in named clients and"
                    + " the elasticsearch keystore for secure settings.",
                "[access_key] setting was deprecated in Elasticsearch and will be removed in a future release."
            );
        }
    }

    public void testInsecureRepositoryCredentials() throws Exception {
        final String repositoryName = "repo-insecure-creds";
        createRepository(
            repositoryName,
            Settings.builder()
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

        assertWarnings(
            "Using s3 access/secret key from repository settings. Instead store these in named clients and"
                + " the elasticsearch keystore for secure settings."
        );
    }

    private void createRepository(final String name, final Settings repositorySettings) {
        assertAcked(
            client().admin()
                .cluster()
                .preparePutRepository(name)
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
        protected S3Repository createRepository(
            RepositoryMetadata metadata,
            NamedXContentRegistry registry,
            ClusterService clusterService,
            BigArrays bigArrays,
            RecoverySettings recoverySettings
        ) {
            return new S3Repository(metadata, registry, getService(), clusterService, bigArrays, recoverySettings) {
                @Override
                protected void assertSnapshotOrGenericThread() {
                    // eliminate thread name check as we create repo manually on test/main threads
                }
            };
        }

        @Override
        S3Service s3Service(Environment environment) {
            return new ProxyS3Service(environment);
        }

        public static final class ClientAndCredentials extends AmazonS3Wrapper {
            final AWSCredentialsProvider credentials;

            ClientAndCredentials(AmazonS3 delegate, AWSCredentialsProvider credentials) {
                super(delegate);
                this.credentials = credentials;
            }
        }

        public static final class ProxyS3Service extends S3Service {

            private static final Logger logger = LogManager.getLogger(ProxyS3Service.class);

            ProxyS3Service(Environment environment) {
                super(environment);
            }

            @Override
            AmazonS3 buildClient(final S3ClientSettings clientSettings) {
                final AmazonS3 client = super.buildClient(clientSettings);
                return new ClientAndCredentials(client, buildCredentials(logger, clientSettings, webIdentityTokenCredentialsProvider));
            }

        }
    }
}
