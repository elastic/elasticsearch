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

import com.microsoft.azure.storage.StorageException;
import org.elasticsearch.cloud.azure.blobstore.AzureBlobStore;
import org.elasticsearch.cloud.azure.storage.AzureStorageService;
import org.elasticsearch.cloud.azure.storage.AzureStorageServiceImpl;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.ESBlobStoreTestCase;
import org.elasticsearch.repositories.RepositoryName;
import org.elasticsearch.repositories.RepositorySettings;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.net.URISyntaxException;

import static org.elasticsearch.cloud.azure.AzureTestUtils.readSettingsFromFile;

/**
 * You must specify {@code -Dtests.thirdparty=true -Dtests.config=/path/to/elasticsearch.yml}
 * in order to run these tests.
 */
@ESIntegTestCase.ThirdParty
public class AzureBlobStoreTests extends ESBlobStoreTestCase {
    @Override
    protected BlobStore newBlobStore() throws IOException {
        try {
            RepositoryName repositoryName = new RepositoryName("azure", "ittest");
            Settings settings = readSettingsFromFile();
            RepositorySettings repositorySettings = new RepositorySettings(settings, Settings.builder().build());
            AzureStorageService storageService = new AzureStorageServiceImpl(settings);
            AzureBlobStore blobStore = new AzureBlobStore(repositoryName, settings, repositorySettings, storageService);
            blobStore.createContainer(blobStore.container());
            return blobStore;
        } catch (URISyntaxException | StorageException e) {
            throw new IOException(e);
        }
    }
}
