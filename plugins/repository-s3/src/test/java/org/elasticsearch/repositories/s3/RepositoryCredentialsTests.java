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
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;

import static org.hamcrest.Matchers.is;

@SuppressForbidden(reason = "test fixture requires System.setProperty")
public class RepositoryCredentialsTests extends ESTestCase {

    static {
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            // required for client settings overwriting
            System.setProperty("es.allow_insecure_settings", "true");
            return null;
        });
    }

    static final class ProxyS3RepositoryPlugin extends S3RepositoryPlugin {

        static final class ClientAndCredentials extends AmazonS3Wrapper {
            final AWSCredentialsProvider credentials;

            ClientAndCredentials(AmazonS3 delegate, AWSCredentialsProvider credentials) {
                super(delegate);
                this.credentials = credentials;
            }

        }

        static final class ProxyS3Service extends S3Service {
            
            private static final Logger logger = LogManager.getLogger(ProxyS3Service.class);
            
            @Override
            AmazonS3 buildClient(final S3ClientSettings clientSettings) {
                final AmazonS3 client = super.buildClient(clientSettings);
                return new ClientAndCredentials(client, buildCredentials(logger, clientSettings));
            }

        }

        ProxyS3RepositoryPlugin(Settings settings) {
            super(settings, new ProxyS3Service());
        }

        @Override
        protected S3Repository createRepository(RepositoryMetaData metadata, Settings settings, NamedXContentRegistry registry) {
            return new S3Repository(metadata, settings, registry, service){
                @Override
                protected void assertSnapshotOrGenericThread() {
                    // eliminate thread name check as we create repo manually on test/main threads
                }
            };
        }
    }

    public void testRepositoryCredentialsOverrideSecureCredentials() throws IOException {
        final int clientsCount = randomIntBetween(0, 4);
        final String[] clientNames = new String[clientsCount + 1];
        clientNames[0] = "default";
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client.default.access_key", "secure_aws_key");
        secureSettings.setString("s3.client.default.secret_key", "secure_aws_secret");
        for (int i = 0; i < clientsCount; i++) {
            final String clientName = "client_" + i;
            secureSettings.setString("s3.client." + clientName + ".access_key", "secure_aws_key_" + i);
            secureSettings.setString("s3.client." + clientName + ".secret_key", "secure_aws_secret_" + i);
            clientNames[i + 1] = clientName;
        }
        final Settings settings = Settings.builder().setSecureSettings(secureSettings).build();
        // repository settings for credentials override node secure settings
        final RepositoryMetaData metadata = new RepositoryMetaData("dummy-repo", "mock", Settings.builder()
                .put(S3Repository.ACCESS_KEY_SETTING.getKey(), "insecure_aws_key")
                .put(S3Repository.SECRET_KEY_SETTING.getKey(), "insecure_aws_secret").build());
        try (S3RepositoryPlugin s3Plugin = new ProxyS3RepositoryPlugin(settings);
             S3Repository s3repo = createAndStartRepository(metadata, s3Plugin);
             AmazonS3Reference s3Ref = ((S3BlobStore) s3repo.blobStore()).clientReference()) {
            final AWSCredentials credentials = ((ProxyS3RepositoryPlugin.ClientAndCredentials) s3Ref.client()).credentials.getCredentials();
            assertThat(credentials.getAWSAccessKeyId(), is("insecure_aws_key"));
            assertThat(credentials.getAWSSecretKey(), is("insecure_aws_secret"));
        }
        assertWarnings(
                "[secret_key] setting was deprecated in Elasticsearch and will be removed in a future release!"
                        + " See the breaking changes documentation for the next major version.",
                "Using s3 access/secret key from repository settings. Instead store these in named clients and"
                        + " the elasticsearch keystore for secure settings.",
                "[access_key] setting was deprecated in Elasticsearch and will be removed in a future release!"
                        + " See the breaking changes documentation for the next major version.");
    }

    public void testRepositoryCredentialsOnly() throws IOException {
        // repository settings for credentials override node secure settings
        final RepositoryMetaData metadata = new RepositoryMetaData("dummy-repo", "mock",
                Settings.builder()
                        .put(S3Repository.ACCESS_KEY_SETTING.getKey(), "insecure_aws_key")
                        .put(S3Repository.SECRET_KEY_SETTING.getKey(), "insecure_aws_secret")
                        .build());
        try (S3RepositoryPlugin s3Plugin = new ProxyS3RepositoryPlugin(Settings.EMPTY);
             S3Repository s3repo = createAndStartRepository(metadata, s3Plugin);
             AmazonS3Reference s3Ref = ((S3BlobStore) s3repo.blobStore()).clientReference()) {
            final AWSCredentials credentials = ((ProxyS3RepositoryPlugin.ClientAndCredentials) s3Ref.client()).credentials.getCredentials();
            assertThat(credentials.getAWSAccessKeyId(), is("insecure_aws_key"));
            assertThat(credentials.getAWSSecretKey(), is("insecure_aws_secret"));
        }
        assertWarnings(
                "[secret_key] setting was deprecated in Elasticsearch and will be removed in a future release!"
                        + " See the breaking changes documentation for the next major version.",
                "Using s3 access/secret key from repository settings. Instead store these in named clients and"
                        + " the elasticsearch keystore for secure settings.",
                "[access_key] setting was deprecated in Elasticsearch and will be removed in a future release!"
                        + " See the breaking changes documentation for the next major version.");
    }

    private S3Repository createAndStartRepository(RepositoryMetaData metadata, S3RepositoryPlugin s3Plugin) {
        final S3Repository repository = s3Plugin.createRepository(metadata, Settings.EMPTY, NamedXContentRegistry.EMPTY);
        repository.start();
        return repository;
    }

    public void testReinitSecureCredentials() throws IOException {
        final String clientName = randomFrom("default", "some_client");
        // initial client node settings
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client." + clientName + ".access_key", "secure_aws_key");
        secureSettings.setString("s3.client." + clientName + ".secret_key", "secure_aws_secret");
        final Settings settings = Settings.builder().setSecureSettings(secureSettings).build();
        // repository settings
        final Settings.Builder builder = Settings.builder();
        final boolean repositorySettings = randomBoolean();
        if (repositorySettings) {
            builder.put(S3Repository.ACCESS_KEY_SETTING.getKey(), "insecure_aws_key");
            builder.put(S3Repository.SECRET_KEY_SETTING.getKey(), "insecure_aws_secret");
        } else {
            builder.put(S3Repository.CLIENT_NAME.getKey(), clientName);
        }
        final RepositoryMetaData metadata = new RepositoryMetaData("dummy-repo", "mock", builder.build());
        try (S3RepositoryPlugin s3Plugin = new ProxyS3RepositoryPlugin(settings);
                S3Repository s3repo = createAndStartRepository(metadata, s3Plugin)) {
            try (AmazonS3Reference s3Ref = ((S3BlobStore) s3repo.blobStore()).clientReference()) {
                final AWSCredentials credentials = ((ProxyS3RepositoryPlugin.ClientAndCredentials) s3Ref.client()).credentials
                        .getCredentials();
                if (repositorySettings) {
                    assertThat(credentials.getAWSAccessKeyId(), is("insecure_aws_key"));
                    assertThat(credentials.getAWSSecretKey(), is("insecure_aws_secret"));
                } else {
                    assertThat(credentials.getAWSAccessKeyId(), is("secure_aws_key"));
                    assertThat(credentials.getAWSSecretKey(), is("secure_aws_secret"));
                }
                // new settings
                final MockSecureSettings newSecureSettings = new MockSecureSettings();
                newSecureSettings.setString("s3.client." + clientName + ".access_key", "new_secret_aws_key");
                newSecureSettings.setString("s3.client." + clientName + ".secret_key", "new_secret_aws_secret");
                final Settings newSettings = Settings.builder().setSecureSettings(newSecureSettings).build();
                // reload S3 plugin settings
                s3Plugin.reload(newSettings);
                // check the not-yet-closed client reference still has the same credentials
                if (repositorySettings) {
                    assertThat(credentials.getAWSAccessKeyId(), is("insecure_aws_key"));
                    assertThat(credentials.getAWSSecretKey(), is("insecure_aws_secret"));
                } else {
                    assertThat(credentials.getAWSAccessKeyId(), is("secure_aws_key"));
                    assertThat(credentials.getAWSSecretKey(), is("secure_aws_secret"));
                }
            }
            // check credentials have been updated
            try (AmazonS3Reference s3Ref = ((S3BlobStore) s3repo.blobStore()).clientReference()) {
                final AWSCredentials newCredentials = ((ProxyS3RepositoryPlugin.ClientAndCredentials) s3Ref.client()).credentials
                        .getCredentials();
                if (repositorySettings) {
                    assertThat(newCredentials.getAWSAccessKeyId(), is("insecure_aws_key"));
                    assertThat(newCredentials.getAWSSecretKey(), is("insecure_aws_secret"));
                } else {
                    assertThat(newCredentials.getAWSAccessKeyId(), is("new_secret_aws_key"));
                    assertThat(newCredentials.getAWSSecretKey(), is("new_secret_aws_secret"));
                }
            }
        }
        if (repositorySettings) {
            assertWarnings(
                    "[secret_key] setting was deprecated in Elasticsearch and will be removed in a future release!"
                            + " See the breaking changes documentation for the next major version.",
                    "Using s3 access/secret key from repository settings. Instead store these in named clients and"
                            + " the elasticsearch keystore for secure settings.",
                    "[access_key] setting was deprecated in Elasticsearch and will be removed in a future release!"
                            + " See the breaking changes documentation for the next major version.");
        }
    }

}
