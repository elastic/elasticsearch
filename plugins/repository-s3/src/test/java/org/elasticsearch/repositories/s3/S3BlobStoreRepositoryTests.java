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

import com.amazonaws.services.s3.AmazonS3;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.rest.AbstractRestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.admin.cluster.RestGetRepositoriesAction;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;

import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Matchers.any;

public class S3BlobStoreRepositoryTests extends ESIntegTestCase {

    private final String bucket = "bucket_" + randomAlphaOfLength(randomIntBetween(1, 10)).toLowerCase(Locale.ROOT);
    private final String client = "client_" + randomAlphaOfLength(randomIntBetween(1, 10)).toLowerCase(Locale.ROOT);
    private final String accessKey = "accessKey_" + randomAlphaOfLength(randomIntBetween(1, 10)).toLowerCase(Locale.ROOT);
    private final String secureKey = "secureKey_" + randomAlphaOfLength(randomIntBetween(1, 10)).toLowerCase(Locale.ROOT);

    protected void createTestRepository(final String name) {
        assertAcked(client().admin().cluster().preparePutRepository(name)
            .setType(S3Repository.TYPE)
            .setVerify(false)
            .setSettings(Settings.builder()
                .put(S3Repository.BUCKET_SETTING.getKey(), bucket)
                .put(InternalAwsS3Service.CLIENT_NAME.getKey(), client)
                .put(S3Repository.ACCESS_KEY_SETTING.getKey(), accessKey)
                .put(S3Repository.SECRET_KEY_SETTING.getKey(), secureKey)));
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(EmptyS3RepositoryPlugin.class);
    }

    public static class EmptyS3RepositoryPlugin extends S3RepositoryPlugin {

        public EmptyS3RepositoryPlugin(final Settings settings) {
            super(settings);
        }

        @Override
        public Map<String, Repository.Factory> getRepositories(final Environment env, final NamedXContentRegistry registry) {
            return Collections.singletonMap(S3Repository.TYPE, (metadata) ->
                new S3Repository(metadata, env.settings(), registry, new InternalAwsS3Service(env.settings(), emptyMap()) {
                    @Override
                    public synchronized AmazonS3 client(final Settings repositorySettings) {
                        final AmazonS3 client = mock(AmazonS3.class);
                        when(client.doesBucketExist(any(String.class))).thenReturn(true);
                        return client;
                    }
                }));
        }
    }

    public void testInsecureRepositoryCredentials() throws Exception {
        final String repositoryName = "testInsecureRepositoryCredentials";
        createTestRepository(repositoryName);
        final NodeClient nodeClient = internalCluster().getInstance(NodeClient.class);
        final RestGetRepositoriesAction getRepoAction = new RestGetRepositoriesAction(Settings.EMPTY, mock(RestController.class),
                internalCluster().getInstance(SettingsFilter.class));
        final RestRequest getRepoRequest = new FakeRestRequest();
        getRepoRequest.params().put("repository", repositoryName);
        final CountDownLatch getRepoLatch = new CountDownLatch(1);
        final AtomicReference<AssertionError> getRepoError = new AtomicReference<>();
        getRepoAction.handleRequest(getRepoRequest, new AbstractRestChannel(getRepoRequest, true) {
            @Override
            public void sendResponse(RestResponse response) {
                try {
                    final String responseContent = response.content().utf8ToString();
                    assertThat(responseContent, containsString(bucket));
                    assertThat(responseContent, containsString(client));
                    assertThat(responseContent, not(containsString(accessKey)));
                    assertThat(responseContent, not(containsString(secureKey)));
                } catch (final AssertionError ex) {
                    getRepoError.set(ex);
                }
                getRepoLatch.countDown();
            }
        }, nodeClient);
        getRepoLatch.await();
        if (getRepoError.get() != null) {
            throw getRepoError.get();
        }
    }
}
