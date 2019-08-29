/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.repositories.s3;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.rest.AbstractRestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.admin.cluster.RestGetRepositoriesAction;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.threadpool.ThreadPool;

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
import static org.mockito.Mockito.mock;

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

        return Settings.builder()
            .setSecureSettings(secureSettings)
            .put(super.nodeSettings())
            .build();
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

        assertWarnings(
            "[secret_key] setting was deprecated in Elasticsearch and will be removed in a future release!"
                + " See the breaking changes documentation for the next major version.",
            "Using s3 access/secret key from repository settings. Instead store these in named clients and"
                + " the elasticsearch keystore for secure settings.",
            "[access_key] setting was deprecated in Elasticsearch and will be removed in a future release!"
                + " See the breaking changes documentation for the next major version.");
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
            assertWarnings(
                "[secret_key] setting was deprecated in Elasticsearch and will be removed in a future release!"
                    + " See the breaking changes documentation for the next major version.",
                "Using s3 access/secret key from repository settings. Instead store these in named clients and"
                    + " the elasticsearch keystore for secure settings.",
                "[access_key] setting was deprecated in Elasticsearch and will be removed in a future release!"
                    + " See the breaking changes documentation for the next major version.");
        }
    }

    public void testInsecureRepositoryCredentials() throws Exception {
        final String repositoryName = "repo-insecure-creds";
        createRepository(repositoryName, Settings.builder()
            .put(S3Repository.ACCESS_KEY_SETTING.getKey(), "insecure_aws_key")
            .put(S3Repository.SECRET_KEY_SETTING.getKey(), "insecure_aws_secret")
            .build());

        final RestRequest fakeRestRequest = new FakeRestRequest();
        fakeRestRequest.params().put("repository", repositoryName);
        final RestGetRepositoriesAction action =
            new RestGetRepositoriesAction(mock(RestController.class), getInstanceFromNode(SettingsFilter.class));

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
                + " the elasticsearch keystore for secure settings.");
    }

    private void createRepository(final String name, final Settings repositorySettings) {
        assertAcked(client().admin().cluster().preparePutRepository(name)
            .setType(S3Repository.TYPE)
            .setVerify(false)
            .setSettings(repositorySettings));
    }

    /**
     * A S3 repository plugin that keeps track of the credentials used to build an AmazonS3 client
     */
    public static final class ProxyS3RepositoryPlugin extends S3RepositoryPlugin {

        public ProxyS3RepositoryPlugin(Settings settings) {
            super(settings, new ProxyS3Service());
        }

        @Override
        protected S3Repository createRepository(RepositoryMetaData metadata,
                                                NamedXContentRegistry registry, ThreadPool threadPool) {
            return new S3Repository(metadata, registry, service, threadPool) {
                @Override
                protected void assertSnapshotOrGenericThread() {
                    // eliminate thread name check as we create repo manually on test/main threads
                }
            };
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

            @Override
            AmazonS3 buildClient(final S3ClientSettings clientSettings) {
                final AmazonS3 client = super.buildClient(clientSettings);
                return new ClientAndCredentials(client, buildCredentials(logger, clientSettings));
            }

        }
    }
}
