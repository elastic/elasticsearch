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

package org.elasticsearch.cloud.azure.storage;

import com.microsoft.azure.storage.LocationMode;
import com.microsoft.azure.storage.StorageException;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.support.PlainBlobMetaData;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.NoSuchFileException;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In memory storage for unit tests
 */
public class AzureStorageServiceMock extends AbstractComponent implements AzureStorageService {

    protected final Map<String, ByteArrayOutputStream> blobs = new ConcurrentHashMap<>();

    public AzureStorageServiceMock() {
        super(Settings.EMPTY);
    }

    @Override
    public boolean doesContainerExist(String account, LocationMode mode, String container) {
        return true;
    }

    @Override
    public void removeContainer(String account, LocationMode mode, String container) {
    }

    @Override
    public void createContainer(String account, LocationMode mode, String container) {
    }

    @Override
    public void deleteFiles(String account, LocationMode mode, String container, String path) {
    }

    @Override
    public boolean blobExists(String account, LocationMode mode, String container, String blob) {
        return blobs.containsKey(blob);
    }

    @Override
    public void deleteBlob(String account, LocationMode mode, String container, String blob) {
        blobs.remove(blob);
    }

    @Override
    public InputStream getInputStream(String account, LocationMode mode, String container, String blob) throws IOException {
        if (!blobExists(account, mode, container, blob)) {
            throw new NoSuchFileException("missing blob [" + blob + "]");
        }
        return new ByteArrayInputStream(blobs.get(blob).toByteArray());
    }

    @Override
    public Map<String, BlobMetaData> listBlobsByPrefix(String account, LocationMode mode, String container, String keyPath, String prefix) {
        MapBuilder<String, BlobMetaData> blobsBuilder = MapBuilder.newMapBuilder();
        blobs.forEach((String blobName, ByteArrayOutputStream bos) -> {
            final String checkBlob;
            if (keyPath != null && !keyPath.isEmpty()) {
                // strip off key path from the beginning of the blob name
                checkBlob = blobName.replace(keyPath, "");
            } else {
                checkBlob = blobName;
            }
            if (prefix == null || startsWithIgnoreCase(checkBlob, prefix)) {
                blobsBuilder.put(blobName, new PlainBlobMetaData(checkBlob, bos.size()));
            }
        });
        return blobsBuilder.immutableMap();
    }

    @Override
    public void moveBlob(String account, LocationMode mode, String container, String sourceBlob, String targetBlob) throws URISyntaxException, StorageException {
        for (String blobName : blobs.keySet()) {
            if (endsWithIgnoreCase(blobName, sourceBlob)) {
                ByteArrayOutputStream outputStream = blobs.get(blobName);
                blobs.put(blobName.replace(sourceBlob, targetBlob), outputStream);
                blobs.remove(blobName);
            }
        }
    }

    @Override
    public void writeBlob(String account, LocationMode mode, String container, String blobName, InputStream inputStream, long blobSize)
        throws URISyntaxException, StorageException {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            blobs.put(blobName, outputStream);
            Streams.copy(inputStream, outputStream);
        } catch (IOException e) {
            throw new StorageException("MOCK", "Error while writing mock stream", e);
        }
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

    /**
     * Test if the given String ends with the specified suffix,
     * ignoring upper/lower case.
     *
     * @param str    the String to check
     * @param suffix the suffix to look for
     * @see java.lang.String#startsWith
     */
    public static boolean endsWithIgnoreCase(String str, String suffix) {
        if (str == null || suffix == null) {
            return false;
        }
        if (str.endsWith(suffix)) {
            return true;
        }
        if (str.length() < suffix.length()) {
            return false;
        }
        String lcStr = str.substring(0, suffix.length()).toLowerCase(Locale.ROOT);
        String lcPrefix = suffix.toLowerCase(Locale.ROOT);
        return lcStr.equals(lcPrefix);
    }
}
