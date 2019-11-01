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
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.repositories.s3.S3ClientSettings.ACCESS_KEY_SETTING;
import static org.elasticsearch.repositories.s3.S3ClientSettings.SECRET_KEY_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

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

        return Settings.builder()
            .setSecureSettings(secureSettings)
            .put(super.nodeSettings())
            .build();
    }

    public void testReinitSecureCredentials() {
        final String clientName = randomFrom("default", "other");

        final Settings.Builder repositorySettings = Settings.builder();
        repositorySettings.put(S3Repository.CLIENT_NAME.getKey(), clientName);

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
            if ("other".equals(clientName)) {
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
            if ("other".equals(clientName)) {
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
            assertThat(newCredentials.getAWSAccessKeyId(), is("new_secret_aws_key"));
            assertThat(newCredentials.getAWSSecretKey(), is("new_secret_aws_secret"));
        }
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
