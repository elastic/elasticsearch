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

package org.elasticsearch.cloud.azure.blobstore;

import com.microsoft.azure.storage.LocationMode;
import com.microsoft.azure.storage.StorageException;
import org.elasticsearch.cloud.azure.storage.AzureStorageService;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.repositories.azure.AzureRepository.Repository;

public class AzureBlobStore extends AbstractComponent implements BlobStore {

    private final AzureStorageService client;

    private final String clientName;
    private final LocationMode locMode;
    private final String container;

    public AzureBlobStore(RepositoryMetaData metadata, Settings settings,
                          AzureStorageService client) throws URISyntaxException, StorageException {
        super(settings);
        this.client = client;
        this.container = Repository.CONTAINER_SETTING.get(metadata.settings());
        this.clientName = Repository.CLIENT_NAME.get(metadata.settings());

        String modeStr = Repository.LOCATION_MODE_SETTING.get(metadata.settings());
        if (Strings.hasLength(modeStr)) {
            this.locMode = LocationMode.valueOf(modeStr.toUpperCase(Locale.ROOT));
        } else {
            this.locMode = LocationMode.PRIMARY_ONLY;
        }
    }

    @Override
    public String toString() {
        return container;
    }

    /**
     * Gets the configured {@link LocationMode} for the Azure storage requests.
     */
    public LocationMode getLocationMode() {
        return locMode;
    }

    @Override
    public BlobContainer blobContainer(BlobPath path) {
        return new AzureBlobContainer(path, this);
    }

    @Override
    public void delete(BlobPath path) {
        String keyPath = path.buildAsString();
        try {
            this.client.deleteFiles(this.clientName, this.locMode, container, keyPath);
        } catch (URISyntaxException | StorageException e) {
            logger.warn("can not remove [{}] in container {{}}: {}", keyPath, container, e.getMessage());
        }
    }

    @Override
    public void close() {
    }

    public boolean doesContainerExist()
    {
        return this.client.doesContainerExist(this.clientName, this.locMode, container);
    }

    public boolean blobExists(String blob) throws URISyntaxException, StorageException
    {
        return this.client.blobExists(this.clientName, this.locMode, container, blob);
    }

    public void deleteBlob(String blob) throws URISyntaxException, StorageException
    {
        this.client.deleteBlob(this.clientName, this.locMode, container, blob);
    }

    public InputStream getInputStream(String blob) throws URISyntaxException, StorageException, IOException
    {
        return this.client.getInputStream(this.clientName, this.locMode, container, blob);
    }

    public Map<String,BlobMetaData> listBlobsByPrefix(String keyPath, String prefix) throws URISyntaxException, StorageException
    {
        return this.client.listBlobsByPrefix(this.clientName, this.locMode, container, keyPath, prefix);
    }

    public void moveBlob(String sourceBlob, String targetBlob) throws URISyntaxException, StorageException
    {
        this.client.moveBlob(this.clientName, this.locMode, container, sourceBlob, targetBlob);
    }

    public void writeBlob(String blobName, InputStream inputStream, long blobSize) throws URISyntaxException, StorageException {
        this.client.writeBlob(this.clientName, this.locMode, container, blobName, inputStream, blobSize);
    }
}
