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

import com.microsoft.azure.storage.LocationMode;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import org.elasticsearch.cloud.azure.storage.AzureStorageService;
import org.elasticsearch.cloud.azure.storage.AzureStorageServiceImpl;
import org.elasticsearch.cloud.azure.storage.AzureStorageSettings;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.elasticsearch.cloud.azure.storage.AzureStorageServiceImpl.blobNameFromUri;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class AzureRepositorySettingsTests extends ESTestCase {

    private AzureRepository azureRepository(Settings settings) throws StorageException, IOException, URISyntaxException {
        Settings internalSettings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath())
            .putArray(Environment.PATH_DATA_SETTING.getKey(), tmpPaths())
            .put(settings)
            .build();
        return new AzureRepository(new RepositoryMetaData("foo", "azure", internalSettings), new Environment(internalSettings),
            NamedXContentRegistry.EMPTY, null);
    }


    public void testReadonlyDefault() throws StorageException, IOException, URISyntaxException {
        assertThat(azureRepository(Settings.EMPTY).isReadOnly(), is(false));
    }

    public void testReadonlyDefaultAndReadonlyOn() throws StorageException, IOException, URISyntaxException {
        assertThat(azureRepository(Settings.builder()
            .put("readonly", true)
            .build()).isReadOnly(), is(true));
    }

    public void testReadonlyWithPrimaryOnly() throws StorageException, IOException, URISyntaxException {
        assertThat(azureRepository(Settings.builder()
            .put(AzureRepository.Repository.LOCATION_MODE_SETTING.getKey(), LocationMode.PRIMARY_ONLY.name())
            .build()).isReadOnly(), is(false));
    }

    public void testReadonlyWithPrimaryOnlyAndReadonlyOn() throws StorageException, IOException, URISyntaxException {
        assertThat(azureRepository(Settings.builder()
            .put(AzureRepository.Repository.LOCATION_MODE_SETTING.getKey(), LocationMode.PRIMARY_ONLY.name())
            .put("readonly", true)
            .build()).isReadOnly(), is(true));
    }

    public void testReadonlyWithSecondaryOnlyAndReadonlyOn() throws StorageException, IOException, URISyntaxException {
        assertThat(azureRepository(Settings.builder()
            .put(AzureRepository.Repository.LOCATION_MODE_SETTING.getKey(), LocationMode.SECONDARY_ONLY.name())
            .put("readonly", true)
            .build()).isReadOnly(), is(true));
    }

    public void testReadonlyWithSecondaryOnlyAndReadonlyOff() throws StorageException, IOException, URISyntaxException {
        assertThat(azureRepository(Settings.builder()
            .put(AzureRepository.Repository.LOCATION_MODE_SETTING.getKey(), LocationMode.SECONDARY_ONLY.name())
            .put("readonly", false)
            .build()).isReadOnly(), is(false));
    }

    public void testReadonlyWithPrimaryAndSecondaryOnlyAndReadonlyOn() throws StorageException, IOException, URISyntaxException {
        assertThat(azureRepository(Settings.builder()
            .put(AzureRepository.Repository.LOCATION_MODE_SETTING.getKey(), LocationMode.PRIMARY_THEN_SECONDARY.name())
            .put("readonly", true)
            .build()).isReadOnly(), is(true));
    }

    public void testReadonlyWithPrimaryAndSecondaryOnlyAndReadonlyOff() throws StorageException, IOException, URISyntaxException {
        assertThat(azureRepository(Settings.builder()
            .put(AzureRepository.Repository.LOCATION_MODE_SETTING.getKey(), LocationMode.PRIMARY_THEN_SECONDARY.name())
            .put("readonly", false)
            .build()).isReadOnly(), is(false));
    }
}
