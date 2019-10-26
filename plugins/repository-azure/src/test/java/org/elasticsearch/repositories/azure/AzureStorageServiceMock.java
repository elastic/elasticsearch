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

import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.support.PlainBlobMetaData;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.internal.io.Streams;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketPermission;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.security.AccessController;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static java.util.Collections.emptyMap;

/**
 * In memory storage for unit tests
 */
public class AzureStorageServiceMock extends AzureStorageService {

    protected final Map<String, ByteArrayOutputStream> blobs = new ConcurrentHashMap<>();

    public AzureStorageServiceMock() {
        super(Settings.EMPTY);
    }

    @Override
    public boolean blobExists(String account, String container, String blob) {
        return blobs.containsKey(blob);
    }

    @Override
    public void deleteBlob(String account, String container, String blob) throws StorageException {
        if (blobs.remove(blob) == null) {
            throw new StorageException("BlobNotFound", "[" + blob + "] does not exist.", 404, null, null);
        }
    }

    @Override
    public InputStream getInputStream(String account, String container, String blob) throws IOException {
        if (!blobExists(account, container, blob)) {
            throw new NoSuchFileException("missing blob [" + blob + "]");
        }
        return AzureStorageService.giveSocketPermissionsToStream(new PermissionRequiringInputStream(blobs.get(blob).toByteArray()));
    }

    @Override
    public Map<String, BlobMetaData> listBlobsByPrefix(String account, String container, String keyPath, String prefix) {
        final var blobsBuilder = new HashMap<String, BlobMetaData>();
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
        return Map.copyOf(blobsBuilder);
    }

    @Override
    public void writeBlob(String account, String container, String blobName, InputStream inputStream, long blobSize,
                          boolean failIfAlreadyExists) throws StorageException, FileAlreadyExistsException {
        if (failIfAlreadyExists && blobs.containsKey(blobName)) {
            throw new FileAlreadyExistsException(blobName);
        }
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
    private static boolean startsWithIgnoreCase(String str, String prefix) {
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

    private static class PermissionRequiringInputStream extends ByteArrayInputStream {

        private PermissionRequiringInputStream(byte[] buf) {
            super(buf);
        }

        @Override
        public synchronized int read() {
            AccessController.checkPermission(new SocketPermission("*", "connect"));
            return super.read();
        }

        @Override
        public int read(byte[] b) throws IOException {
            AccessController.checkPermission(new SocketPermission("*", "connect"));
            return super.read(b);
        }

        @Override
        public synchronized int read(byte[] b, int off, int len) {
            AccessController.checkPermission(new SocketPermission("*", "connect"));
            return super.read(b, off, len);
        }
    }

    @Override
    public Tuple<CloudBlobClient, Supplier<OperationContext>> client(String clientName) {
        return null;
    }

    @Override
    public Map<String, AzureStorageSettings> refreshAndClearCache(Map<String, AzureStorageSettings> clientsSettings) {
        return emptyMap();
    }
}
