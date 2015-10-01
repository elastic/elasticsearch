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

package org.elasticsearch.cloud.azure;

import com.microsoft.azure.storage.StorageException;
import org.elasticsearch.cloud.azure.storage.AzureStorageService;
import org.elasticsearch.cloud.azure.storage.AzureStorageService.Storage;
import org.elasticsearch.cloud.azure.storage.AzureStorageServiceMock;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugin.cloud.azure.CloudAzurePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.test.store.MockFSDirectoryService;
import org.junit.After;
import org.junit.Before;

import java.net.URISyntaxException;
import java.util.Collection;

public abstract class AbstractAzureRepositoryServiceTestCase extends AbstractAzureTestCase {

    public static class TestPlugin extends Plugin {
        @Override
        public String name() {
            return "mock-stoarge-service";
        }
        @Override
        public String description() {
            return "plugs in a mock storage service for testing";
        }
        public void onModule(AzureModule azureModule) {
            azureModule.storageServiceImpl = AzureStorageServiceMock.class;
        }
    }

    protected String basePath;
    private Class<? extends AzureStorageService> mock;

    public AbstractAzureRepositoryServiceTestCase(String basePath) {
        this.basePath = basePath;
    }

    /**
     * Deletes repositories, supports wildcard notation.
     */
    public static void wipeRepositories(String... repositories) {
        // if nothing is provided, delete all
        if (repositories.length == 0) {
            repositories = new String[]{"*"};
        }
        for (String repository : repositories) {
            try {
                client().admin().cluster().prepareDeleteRepository(repository).execute().actionGet();
            } catch (RepositoryMissingException ex) {
                // ignore
            }
        }
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder builder = Settings.settingsBuilder()
                .put(Storage.API_IMPLEMENTATION, mock)
                .put(Storage.CONTAINER, "snapshots");

        // We use sometime deprecated settings in tests
        builder.put(Storage.ACCOUNT, "mock_azure_account")
                .put(Storage.KEY, "mock_azure_key");

        return builder.build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(CloudAzurePlugin.class, TestPlugin.class);
    }

    @Override
    public Settings indexSettings() {
        // During restore we frequently restore index to exactly the same state it was before, that might cause the same
        // checksum file to be written twice during restore operation
        return Settings.builder().put(super.indexSettings())
                .put(MockFSDirectoryService.RANDOM_PREVENT_DOUBLE_WRITE, false)
                .put(MockFSDirectoryService.RANDOM_NO_DELETE_OPEN_FILE, false)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .build();
    }

    @Before @After
    public final void wipe() throws StorageException, URISyntaxException {
        wipeRepositories();
        cleanRepositoryFiles(basePath);
    }

    /**
     * Purge the test container
     */
    public void cleanRepositoryFiles(String path) throws StorageException, URISyntaxException {
        String container = internalCluster().getInstance(Settings.class).get("repositories.azure.container");
        logger.info("--> remove blobs in container [{}]", container);
        AzureStorageService client = internalCluster().getInstance(AzureStorageService.class);
        client.deleteFiles(container, path);
    }
}
