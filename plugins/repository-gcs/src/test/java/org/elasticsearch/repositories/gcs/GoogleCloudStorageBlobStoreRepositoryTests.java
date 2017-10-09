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

package org.elasticsearch.repositories.gcs;

import com.google.api.services.storage.Storage;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.blobstore.ESBlobStoreRepositoryIntegTestCase;
import org.junit.BeforeClass;

import java.net.SocketPermission;
import java.security.AccessController;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class GoogleCloudStorageBlobStoreRepositoryTests extends ESBlobStoreRepositoryIntegTestCase {

    private static final String BUCKET = "gcs-repository-test";

    // Static storage client shared among all nodes in order to act like a remote repository service:
    // all nodes must see the same content
    private static final AtomicReference<Storage> storage = new AtomicReference<>();

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockGoogleCloudStoragePlugin.class);
    }

    @Override
    protected void createTestRepository(String name) {
        assertAcked(client().admin().cluster().preparePutRepository(name)
                .setType(GoogleCloudStorageRepository.TYPE)
                .setSettings(Settings.builder()
                        .put("bucket", BUCKET)
                        .put("base_path", GoogleCloudStorageBlobStoreRepositoryTests.class.getSimpleName())
                        .put("service_account", "_default_")
                        .put("compress", randomBoolean())
                        .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));
    }

    @BeforeClass
    public static void setUpStorage() {
        storage.set(MockHttpTransport.newStorage(BUCKET, GoogleCloudStorageBlobStoreRepositoryTests.class.getName()));
    }

    public static class MockGoogleCloudStoragePlugin extends GoogleCloudStoragePlugin {
        public MockGoogleCloudStoragePlugin() {
            super(Settings.EMPTY);
        }
        @Override
        protected GoogleCloudStorageService createStorageService(Environment environment) {
            return new MockGoogleCloudStorageService();
        }
    }

    public static class MockGoogleCloudStorageService implements GoogleCloudStorageService {
        @Override
        public Storage createClient(String accountName, String application,
                                    TimeValue connectTimeout, TimeValue readTimeout) throws Exception {
            // The actual impl might open a connection. So check we have permission when this call is made.
            AccessController.checkPermission(new SocketPermission("*", "connect"));
            return storage.get();
        }
    }

    public void testChunkSize() {
        // default chunk size
        RepositoryMetaData repositoryMetaData = new RepositoryMetaData("repo", GoogleCloudStorageRepository.TYPE, Settings.EMPTY);
        ByteSizeValue chunkSize = GoogleCloudStorageRepository.getSetting(GoogleCloudStorageRepository.CHUNK_SIZE, repositoryMetaData);
        assertEquals(GoogleCloudStorageRepository.MAX_CHUNK_SIZE, chunkSize);

        // chunk size in settings
        int size = randomIntBetween(1, 100);
        repositoryMetaData = new RepositoryMetaData("repo", GoogleCloudStorageRepository.TYPE,
                                                       Settings.builder().put("chunk_size", size + "mb").build());
        chunkSize = GoogleCloudStorageRepository.getSetting(GoogleCloudStorageRepository.CHUNK_SIZE, repositoryMetaData);
        assertEquals(new ByteSizeValue(size, ByteSizeUnit.MB), chunkSize);

        // zero bytes is not allowed
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            RepositoryMetaData repoMetaData = new RepositoryMetaData("repo", GoogleCloudStorageRepository.TYPE,
                                                                        Settings.builder().put("chunk_size", "0").build());
            GoogleCloudStorageRepository.getSetting(GoogleCloudStorageRepository.CHUNK_SIZE, repoMetaData);
        });
        assertEquals("Failed to parse value [0] for setting [chunk_size] must be >= 1b", e.getMessage());

        // negative bytes not allowed
        e = expectThrows(IllegalArgumentException.class, () -> {
            RepositoryMetaData repoMetaData = new RepositoryMetaData("repo", GoogleCloudStorageRepository.TYPE,
                                                                        Settings.builder().put("chunk_size", "-1").build());
            GoogleCloudStorageRepository.getSetting(GoogleCloudStorageRepository.CHUNK_SIZE, repoMetaData);
        });
        assertEquals("Failed to parse value [-1] for setting [chunk_size] must be >= 1b", e.getMessage());

        // greater than max chunk size not allowed
        e = expectThrows(IllegalArgumentException.class, () -> {
            RepositoryMetaData repoMetaData = new RepositoryMetaData("repo", GoogleCloudStorageRepository.TYPE,
                                                                        Settings.builder().put("chunk_size", "101mb").build());
            GoogleCloudStorageRepository.getSetting(GoogleCloudStorageRepository.CHUNK_SIZE, repoMetaData);
        });
        assertEquals("Failed to parse value [101mb] for setting [chunk_size] must be <= 100mb", e.getMessage());
    }
}
