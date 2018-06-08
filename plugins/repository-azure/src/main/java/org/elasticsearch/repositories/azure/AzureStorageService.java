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
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.Map;

/**
 * Azure Storage Service interface
 * @see AzureStorageServiceImpl for Azure REST API implementation
 */
public interface AzureStorageService {

    ByteSizeValue MIN_CHUNK_SIZE = new ByteSizeValue(1, ByteSizeUnit.BYTES);
    ByteSizeValue MAX_CHUNK_SIZE = new ByteSizeValue(64, ByteSizeUnit.MB);

    boolean doesContainerExist(String account, LocationMode mode, String container);

    void removeContainer(String account, LocationMode mode, String container) throws URISyntaxException, StorageException;

    void createContainer(String account, LocationMode mode, String container) throws URISyntaxException, StorageException;

    void deleteFiles(String account, LocationMode mode, String container, String path) throws URISyntaxException, StorageException;

    boolean blobExists(String account, LocationMode mode, String container, String blob) throws URISyntaxException, StorageException;

    void deleteBlob(String account, LocationMode mode, String container, String blob) throws URISyntaxException, StorageException;

    InputStream getInputStream(String account, LocationMode mode, String container, String blob)
        throws URISyntaxException, StorageException, IOException;

    Map<String,BlobMetaData> listBlobsByPrefix(String account, LocationMode mode, String container, String keyPath, String prefix)
        throws URISyntaxException, StorageException;

    void writeBlob(String account, LocationMode mode, String container, String blobName, InputStream inputStream, long blobSize) throws
        URISyntaxException, StorageException;

    static InputStream giveSocketPermissionsToStream(InputStream stream) {
        return new InputStream() {
            @Override
            public int read() throws IOException {
                return SocketAccess.doPrivilegedIOException(stream::read);
            }

            @Override
            public int read(byte[] b) throws IOException {
                return SocketAccess.doPrivilegedIOException(() -> stream.read(b));
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                return SocketAccess.doPrivilegedIOException(() -> stream.read(b, off, len));
            }
        };
    }
}
