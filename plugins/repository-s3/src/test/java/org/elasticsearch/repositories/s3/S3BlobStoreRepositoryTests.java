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
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.StorageClass;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.blobstore.ESBlobStoreRepositoryIntegTestCase;
import org.elasticsearch.rest.AbstractRestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.admin.cluster.RestGetRepositoriesAction;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;

public class S3BlobStoreRepositoryTests extends ESBlobStoreRepositoryIntegTestCase {

    private static final ConcurrentMap<String, byte[]> blobs = new ConcurrentHashMap<>();
    private static String bucket;
    private static String client;
    private static ByteSizeValue bufferSize;
    private static boolean serverSideEncryption;
    private static String cannedACL;
    private static String storageClass;

    @BeforeClass
    public static void setUpRepositorySettings() {
        bucket = randomAlphaOfLength(randomIntBetween(1, 10)).toLowerCase(Locale.ROOT);
        client = randomAlphaOfLength(randomIntBetween(1, 10)).toLowerCase(Locale.ROOT);
        bufferSize = new ByteSizeValue(randomIntBetween(5, 50), ByteSizeUnit.MB);
        serverSideEncryption = randomBoolean();
        if (randomBoolean()) {
            cannedACL = randomFrom(CannedAccessControlList.values()).toString();
        }
        if (randomBoolean()) {
            storageClass = randomValueOtherThan(StorageClass.Glacier, () -> randomFrom(StorageClass.values())).toString();
        }
    }

    @AfterClass
    public static void wipeRepository() {
        blobs.clear();
    }

    @Override
    protected void createTestRepository(final String name, boolean verify) {
        assertAcked(client().admin().cluster().preparePutRepository(name)
            .setType(S3Repository.TYPE)
            .setVerify(verify)
            .setSettings(Settings.builder()
                .put(S3Repository.BUCKET_SETTING.getKey(), bucket)
                .put(S3Repository.BUFFER_SIZE_SETTING.getKey(), bufferSize)
                .put(S3Repository.SERVER_SIDE_ENCRYPTION_SETTING.getKey(), serverSideEncryption)
                .put(S3Repository.CANNED_ACL_SETTING.getKey(), cannedACL)
                .put(S3Repository.STORAGE_CLASS_SETTING.getKey(), storageClass)
                .put(S3Repository.ACCESS_KEY_SETTING.getKey(), "not_used_but_this_is_a_secret")
                .put(S3Repository.SECRET_KEY_SETTING.getKey(), "not_used_but_this_is_a_secret")));
    }

    @Override
    protected void afterCreationCheck(Repository repository) {
        assertThat(repository, instanceOf(S3Repository.class));
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(TestS3RepositoryPlugin.class);
    }

    public static class TestS3RepositoryPlugin extends S3RepositoryPlugin {

        public TestS3RepositoryPlugin(final Settings settings) {
            super(settings);
        }

        @Override
        public Map<String, Repository.Factory> getRepositories(final Environment env, final NamedXContentRegistry registry) {
            return Collections.singletonMap(S3Repository.TYPE,
                    (metadata) -> new S3Repository(metadata, env.settings(), registry, new S3Service() {
                        @Override
                        AmazonS3 buildClient(S3ClientSettings clientSettings) {
                            return new MockAmazonS3(blobs, bucket, serverSideEncryption, cannedACL, storageClass);
                        }
                    }));
        }
    }

    public void testInsecureRepositoryCredentials() throws Exception {
        final String repositoryName = "testInsecureRepositoryCredentials";
        createAndCheckTestRepository(repositoryName);
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
                    assertThat(response.content().utf8ToString(), not(containsString("not_used_but_this_is_a_secret")));
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
