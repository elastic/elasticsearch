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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cloud.azure.AzureStorageService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.support.PlainBlobMetaData;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In memory storage for unit tests
 */
public class AzureStorageServiceMock extends AbstractLifecycleComponent<AzureStorageServiceMock>
        implements AzureStorageService {

    protected Map<String, byte[]> blobs = new ConcurrentHashMap<String, byte[]>();

    @Inject
    protected AzureStorageServiceMock(Settings settings) {
        super(settings);
    }

    @Override
    public boolean doesContainerExist(String container) {
        return true;
    }

    @Override
    public void removeContainer(String container) {
    }

    @Override
    public void createContainer(String container) {
    }

    @Override
    public void deleteFiles(String container, String path) {
    }

    @Override
    public boolean blobExists(String container, String blob) {
        return blobs.containsKey(blob);
    }

    @Override
    public void deleteBlob(String container, String blob) {
        blobs.remove(blob);
    }

    @Override
    public InputStream getInputStream(String container, String blob) {
        return new ByteArrayInputStream(blobs.get(blob));
    }

    @Override
    public ImmutableMap<String, BlobMetaData> listBlobsByPrefix(String container, String keyPath, String prefix) {
        ImmutableMap.Builder<String, BlobMetaData> blobsBuilder = ImmutableMap.builder();
        for (String blobName : blobs.keySet()) {
            if (Strings.startsWithIgnoreCase(blobName, prefix)) {
                blobsBuilder.put(blobName, new PlainBlobMetaData(blobName, blobs.get(blobName).length));
            }
        }
        ImmutableMap<String, BlobMetaData> map = blobsBuilder.build();
        return map;
    }

    @Override
    public void putObject(String container, String blob, InputStream is, long length) {
        try {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();

            int nRead;
            byte[] data = new byte[65535];

            while ((nRead = is.read(data, 0, data.length)) != -1) {
                buffer.write(data, 0, nRead);
            }

            buffer.flush();

            blobs.put(blob, buffer.toByteArray());
        } catch (IOException e) {
        }
    }

    @Override
    protected void doStart() throws ElasticsearchException {
    }

    @Override
    protected void doStop() throws ElasticsearchException {
    }

    @Override
    protected void doClose() throws ElasticsearchException {
    }
}
