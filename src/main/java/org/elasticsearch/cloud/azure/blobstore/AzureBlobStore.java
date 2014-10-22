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

import com.microsoft.windowsazure.services.core.ServiceException;
import com.microsoft.windowsazure.services.core.storage.StorageException;
import org.elasticsearch.cloud.azure.AzureStorageService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;

import java.net.URISyntaxException;

/**
 *
 */
public class AzureBlobStore extends AbstractComponent implements BlobStore {

    private final AzureStorageService client;

    private final String container;

    public AzureBlobStore(Settings settings, AzureStorageService client, String container) throws URISyntaxException, StorageException {
        super(settings);
        this.client = client;
        this.container = container;

        if (!client.doesContainerExist(container)) {
            client.createContainer(container);
        }
    }

    @Override
    public String toString() {
        return container;
    }

    public AzureStorageService client() {
        return client;
    }

    public String container() {
        return container;
    }

    @Override
    public BlobContainer blobContainer(BlobPath path) {
        return new AzureBlobContainer(path, this);
    }

    @Override
    public void delete(BlobPath path) {
        String keyPath = path.buildAsString("/");
        if (!keyPath.isEmpty()) {
            keyPath = keyPath + "/";
        }

        try {
            client.deleteFiles(container, keyPath);
        } catch (URISyntaxException e) {
            logger.warn("can not remove [{}] in container {{}}: {}", keyPath, container, e.getMessage());
        } catch (StorageException e) {
            logger.warn("can not remove [{}] in container {{}}: {}", keyPath, container, e.getMessage());
        } catch (ServiceException e) {
            logger.warn("can not remove [{}] in container {{}}: {}", keyPath, container, e.getMessage());
        }
    }

    @Override
    public void close() {
    }
}
