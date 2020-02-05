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
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.repositories.azure.AzureRepository.Repository;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;

public class AzureBlobStore implements BlobStore {

    private final AzureStorageService service;
    private final ThreadPool threadPool;

    private final String clientName;
    private final String container;
    private final LocationMode locationMode;

    public AzureBlobStore(RepositoryMetaData metadata, AzureStorageService service, ThreadPool threadPool) {
        this.container = Repository.CONTAINER_SETTING.get(metadata.settings());
        this.clientName = Repository.CLIENT_NAME.get(metadata.settings());
        this.service = service;
        this.threadPool = threadPool;
        // locationMode is set per repository, not per client
        this.locationMode = Repository.LOCATION_MODE_SETTING.get(metadata.settings());
        final Map<String, AzureStorageSettings> prevSettings = this.service.refreshAndClearCache(emptyMap());
        final Map<String, AzureStorageSettings> newSettings = AzureStorageSettings.overrideLocationMode(prevSettings, this.locationMode);
        this.service.refreshAndClearCache(newSettings);
    }

    @Override
    public String toString() {
        return container;
    }

    public AzureStorageService getService() {
        return service;
    }

    /**
     * Gets the configured {@link LocationMode} for the Azure storage requests.
     */
    public LocationMode getLocationMode() {
        return locationMode;
    }

    @Override
    public BlobContainer blobContainer(BlobPath path) {
        return new AzureBlobContainer(path, this, threadPool);
    }

    @Override
    public void close() {
    }

    public boolean blobExists(String blob) throws URISyntaxException, StorageException, IOException {
        return service.blobExists(clientName, container, blob);
    }

    public void deleteBlob(String blob) throws URISyntaxException, StorageException, IOException {
        service.deleteBlob(clientName, container, blob);
    }

    public DeleteResult deleteBlobDirectory(String path, Executor executor)
            throws URISyntaxException, StorageException, IOException {
        return service.deleteBlobDirectory(clientName, container, path, executor);
    }

    public InputStream getInputStream(String blob) throws URISyntaxException, StorageException, IOException {
        return service.getInputStream(clientName, container, blob);
    }

    public Map<String, BlobMetaData> listBlobsByPrefix(String keyPath, String prefix)
        throws URISyntaxException, StorageException, IOException {
        return service.listBlobsByPrefix(clientName, container, keyPath, prefix);
    }

    public Map<String, BlobContainer> children(BlobPath path) throws URISyntaxException, StorageException, IOException {
        return Collections.unmodifiableMap(service.children(clientName, container, path).stream().collect(
            Collectors.toMap(Function.identity(), name -> new AzureBlobContainer(path.add(name), this, threadPool))));
    }

    public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
        throws URISyntaxException, StorageException, IOException {
        service.writeBlob(this.clientName, container, blobName, inputStream, blobSize, failIfAlreadyExists);
    }
}
