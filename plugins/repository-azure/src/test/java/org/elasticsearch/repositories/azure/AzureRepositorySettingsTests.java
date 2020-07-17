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
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.repositories.blobstore.BlobStoreTestUtil;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class AzureRepositorySettingsTests extends ESTestCase {

    private AzureRepository azureRepository(Settings settings) {
        Settings internalSettings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath())
            .putList(Environment.PATH_DATA_SETTING.getKey(), tmpPaths())
            .put(settings)
            .build();
        final AzureRepository azureRepository = new AzureRepository(new RepositoryMetadata("foo", "azure", internalSettings),
            NamedXContentRegistry.EMPTY, mock(AzureStorageService.class), BlobStoreTestUtil.mockClusterService(),
            new RecoverySettings(settings, new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)));
        assertThat(azureRepository.getBlobStore(), is(nullValue()));
        return azureRepository;
    }

    public void testReadonlyDefault() {
        assertThat(azureRepository(Settings.EMPTY).isReadOnly(), is(false));
    }

    public void testReadonlyDefaultAndReadonlyOn() {
        assertThat(azureRepository(Settings.builder()
            .put("readonly", true)
            .build()).isReadOnly(), is(true));
    }

    public void testReadonlyWithPrimaryOnly() {
        assertThat(azureRepository(Settings.builder()
            .put(AzureRepository.Repository.LOCATION_MODE_SETTING.getKey(), LocationMode.PRIMARY_ONLY.name())
            .build()).isReadOnly(), is(false));
    }

    public void testReadonlyWithPrimaryOnlyAndReadonlyOn() {
        assertThat(azureRepository(Settings.builder()
            .put(AzureRepository.Repository.LOCATION_MODE_SETTING.getKey(), LocationMode.PRIMARY_ONLY.name())
            .put("readonly", true)
            .build()).isReadOnly(), is(true));
    }

    public void testReadonlyWithSecondaryOnlyAndReadonlyOn() {
        assertThat(azureRepository(Settings.builder()
            .put(AzureRepository.Repository.LOCATION_MODE_SETTING.getKey(), LocationMode.SECONDARY_ONLY.name())
            .put("readonly", true)
            .build()).isReadOnly(), is(true));
    }

    public void testReadonlyWithSecondaryOnlyAndReadonlyOff() {
        assertThat(azureRepository(Settings.builder()
            .put(AzureRepository.Repository.LOCATION_MODE_SETTING.getKey(), LocationMode.SECONDARY_ONLY.name())
            .put("readonly", false)
            .build()).isReadOnly(), is(false));
    }

    public void testReadonlyWithPrimaryAndSecondaryOnlyAndReadonlyOn() {
        assertThat(azureRepository(Settings.builder()
            .put(AzureRepository.Repository.LOCATION_MODE_SETTING.getKey(), LocationMode.PRIMARY_THEN_SECONDARY.name())
            .put("readonly", true)
            .build()).isReadOnly(), is(true));
    }

    public void testReadonlyWithPrimaryAndSecondaryOnlyAndReadonlyOff() {
        assertThat(azureRepository(Settings.builder()
            .put(AzureRepository.Repository.LOCATION_MODE_SETTING.getKey(), LocationMode.PRIMARY_THEN_SECONDARY.name())
            .put("readonly", false)
            .build()).isReadOnly(), is(false));
    }

    public void testChunkSize() {
        // default chunk size
        AzureRepository azureRepository = azureRepository(Settings.EMPTY);
        assertEquals(AzureStorageService.MAX_CHUNK_SIZE, azureRepository.chunkSize());

        // chunk size in settings
        int size = randomIntBetween(1, 256);
        azureRepository = azureRepository(Settings.builder().put("chunk_size", size + "mb").build());
        assertEquals(new ByteSizeValue(size, ByteSizeUnit.MB), azureRepository.chunkSize());

        // zero bytes is not allowed
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
            azureRepository(Settings.builder().put("chunk_size", "0").build()));
        assertEquals("failed to parse value [0] for setting [chunk_size], must be >= [1b]", e.getMessage());

        // negative bytes not allowed
        e = expectThrows(IllegalArgumentException.class, () ->
            azureRepository(Settings.builder().put("chunk_size", "-1").build()));
        assertEquals("failed to parse value [-1] for setting [chunk_size], must be >= [1b]", e.getMessage());

        // greater than max chunk size not allowed
        e = expectThrows(IllegalArgumentException.class, () ->
                azureRepository(Settings.builder().put("chunk_size", "6tb").build()));
        assertEquals("failed to parse value [6tb] for setting [chunk_size], must be <= ["
                + AzureStorageService.MAX_CHUNK_SIZE.getStringRep() + "]", e.getMessage());
    }

}
