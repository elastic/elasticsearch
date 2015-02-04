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

import com.microsoft.windowsazure.services.core.storage.StorageException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cloud.azure.AzureStorageService;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.support.PlainBlobMetaData;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

import java.io.*;
import java.net.URISyntaxException;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In memory storage for unit tests
 */
public class AzureStorageServiceMock extends AbstractLifecycleComponent<AzureStorageServiceMock>
        implements AzureStorageService {

    protected Map<String, ByteArrayOutputStream> blobs = new ConcurrentHashMap<>();

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
        return new ByteArrayInputStream(blobs.get(blob).toByteArray());
    }

    @Override
    public OutputStream getOutputStream(String container, String blob) throws URISyntaxException, StorageException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        blobs.put(blob, outputStream);
        return outputStream;
    }

    @Override
    public ImmutableMap<String, BlobMetaData> listBlobsByPrefix(String container, String keyPath, String prefix) {
        ImmutableMap.Builder<String, BlobMetaData> blobsBuilder = ImmutableMap.builder();
        for (String blobName : blobs.keySet()) {
            if (startsWithIgnoreCase(blobName, prefix)) {
                blobsBuilder.put(blobName, new PlainBlobMetaData(blobName, blobs.get(blobName).size()));
            }
        }
        ImmutableMap<String, BlobMetaData> map = blobsBuilder.build();
        return map;
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

    /**
     * Test if the given String starts with the specified prefix,
     * ignoring upper/lower case.
     *
     * @param str    the String to check
     * @param prefix the prefix to look for
     * @see java.lang.String#startsWith
     */
    public static boolean startsWithIgnoreCase(String str, String prefix) {
        if (str == null || prefix == null) {
            return false;
        }
        if (str.startsWith(prefix)) {
            return true;
        }
        if (str.length() < prefix.length()) {
            return false;
        }
        String lcStr = str.substring(0, prefix.length()).toLowerCase(Locale.ROOT);
        String lcPrefix = prefix.toLowerCase(Locale.ROOT);
        return lcStr.equals(lcPrefix);
    }
}
