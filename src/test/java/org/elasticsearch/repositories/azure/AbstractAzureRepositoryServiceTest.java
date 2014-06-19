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

package org.elasticsearch.repositories.azure;

import com.microsoft.windowsazure.services.core.ServiceException;
import com.microsoft.windowsazure.services.core.storage.StorageException;
import org.elasticsearch.cloud.azure.AbstractAzureTest;
import org.elasticsearch.cloud.azure.AzureStorageService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.test.store.MockDirectoryHelper;
import org.junit.After;
import org.junit.Before;

import java.net.URISyntaxException;

public abstract class AbstractAzureRepositoryServiceTest extends AbstractAzureTest {

    protected String basePath;
    private Class<? extends AzureStorageService> mock;

    public AbstractAzureRepositoryServiceTest(Class<? extends AzureStorageService> mock,
                                              String basePath) {
        // We want to inject the Azure API Mock
        this.mock = mock;
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
        ImmutableSettings.Builder builder = ImmutableSettings.settingsBuilder()
                .put("plugins." + PluginsService.LOAD_PLUGIN_FROM_CLASSPATH, true)
                .put("cloud.azure." + AzureStorageService.Fields.ACCOUNT, "mock_azure_account")
                .put("cloud.azure." + AzureStorageService.Fields.KEY, "mock_azure_key")
                .put("repositories.azure.api.impl", mock)
                .put("repositories.azure.container", "snapshots");

        return builder.build();
    }

    @Override
    public Settings indexSettings() {
        // During restore we frequently restore index to exactly the same state it was before, that might cause the same
        // checksum file to be written twice during restore operation
        return ImmutableSettings.builder().put(super.indexSettings())
                .put(MockDirectoryHelper.RANDOM_PREVENT_DOUBLE_WRITE, false)
                .put(MockDirectoryHelper.RANDOM_NO_DELETE_OPEN_FILE, false)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .build();
    }

    @Before @After
    public final void wipe() throws StorageException, ServiceException, URISyntaxException {
        wipeRepositories();
        cleanRepositoryFiles(basePath);
    }

    /**
     * Purge the test container
     */
    public void cleanRepositoryFiles(String path) throws StorageException, ServiceException, URISyntaxException {
        String container = internalCluster().getInstance(Settings.class).get("repositories.azure.container");
        logger.info("--> remove blobs in container [{}]", container);
        AzureStorageService client = internalCluster().getInstance(AzureStorageService.class);
        client.deleteFiles(container, path);
    }
}
