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

package org.elasticsearch.cloud.qiniu.blobstore;

import com.qiniu.common.QiniuException;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.storage.model.FileListing;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStoreException;
import org.elasticsearch.common.blobstore.support.AbstractBlobContainer;
import org.elasticsearch.common.blobstore.support.PlainBlobMetaData;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.io.Streams;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;

public class KodoBlobContainer extends AbstractBlobContainer {

    protected final KodoBlobStore blobStore;

    protected final String keyPath;

    public KodoBlobContainer(BlobPath path, KodoBlobStore blobStore) {
        super(path);
        this.blobStore = blobStore;
        this.keyPath = path.buildAsString();
    }

    @Override
    public boolean blobExists(String blobName) {
        try {
            return doPrivileged(() -> {
                try {
                    return blobStore.client().isBucketExist(blobStore.bucket());
                } catch (QiniuException e) {
                    return false;
                }
            });
        } catch (QiniuException e) {
            return false;
        } catch (Exception e) {
            throw new BlobStoreException("failed to check if blob exists", e);
        }
    }

    @Override
    public InputStream readBlob(String blobName) throws IOException {
        return blobStore.client().getObject(buildKey(blobName));
    }

    @Override
    public void writeBlob(String blobName, InputStream inputStream, long blobSize) throws IOException {
        if (blobExists(blobName)) {
            throw new FileAlreadyExistsException("blob [" + blobName + "] already exists, cannot overwrite");
        }
        try (OutputStream stream = createOutput(blobName)) {
            Streams.copy(inputStream, stream);
        }
    }

    @Override
    public void deleteBlob(String blobName) throws IOException {
        if (!blobExists(blobName)) {
            throw new NoSuchFileException("Blob [" + blobName + "] does not exist");
        }
        try {
            blobStore.client().delete(blobStore.bucket(), buildKey(blobName));
        } catch (QiniuException e) {
            throw new IOException("Exception when deleting blob [" + blobName + "]", e);
        }
    }

    private OutputStream createOutput(final String blobName) throws IOException {
        return new DefaultKodoOutputStream(blobStore, blobStore.bucket(), buildKey(blobName),
                blobStore.bufferSizeInBytes(), blobStore.numberOfRetries());
    }

    @Override
    public Map<String, BlobMetaData> listBlobsByPrefix(@Nullable String blobNamePrefix) throws IOException {
        MapBuilder<String, BlobMetaData> mapBuilder = MapBuilder.newMapBuilder();
        String marker = "";

        FileListing fileListing;
        do {
            fileListing = blobStore.client().listObjects(blobStore.bucket(), buildKey(blobNamePrefix), marker, 20);
            marker = fileListing.marker;
            FileInfo[] fileInfos = fileListing.items;

            for (FileInfo fileInfo : fileInfos) {
                String name = fileInfo.key.substring(this.keyPath.length());
                mapBuilder.put(name, new PlainBlobMetaData(name, fileInfo.fsize));
            }
        } while (!fileListing.isEOF());

        return mapBuilder.immutableMap();
    }

    @Override
    public void move(String sourceBlobName, String targetBlobName) throws IOException {
        blobStore.client().move(blobStore.bucket(), buildKey(sourceBlobName), buildKey(targetBlobName));
    }

    @Override
    public Map<String, BlobMetaData> listBlobs() throws IOException {
        return listBlobsByPrefix(null);
    }

    protected String buildKey(String blobName) {
        return keyPath + blobName;
    }

    /**
     * +     * Executes a {@link PrivilegedExceptionAction} with privileges enabled.
     * +
     */
    <T> T doPrivileged(PrivilegedExceptionAction<T> operation) throws IOException {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }
        try {
            return AccessController.doPrivileged(operation);
        } catch (PrivilegedActionException e) {
            throw (IOException) e.getException();
        }
    }
}
