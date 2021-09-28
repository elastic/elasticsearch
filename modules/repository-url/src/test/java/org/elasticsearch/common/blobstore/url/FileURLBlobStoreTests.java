/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.blobstore.url;

import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.url.http.URLHttpClient;
import org.elasticsearch.common.blobstore.url.http.URLHttpClientSettings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.mockito.Mockito.mock;

public class FileURLBlobStoreTests extends AbstractURLBlobStoreTests {
    private static byte[] data;
    private static String blobName;
    private static URLBlobStore blobStore;
    private static Path file;

    @BeforeClass
    public static void setUpData() throws Exception {
        data = randomByteArrayOfLength(512);
        file = createTempFile();
        blobName = file.getFileName().toString();
        Files.write(file, data);
        blobStore = new URLBlobStore(Settings.EMPTY, file.getParent().toUri().toURL(), mock(URLHttpClient.class),
            mock(URLHttpClientSettings.class));
    }

    @Override
    BytesArray getOriginalData() {
        return new BytesArray(data);
    }

    @Override
    BlobContainer getBlobContainer() {
        return blobStore.blobContainer(BlobPath.EMPTY);
    }

    @Override
    String getBlobName() {
        return blobName;
    }

    @Override
    public void testURLBlobStoreCanReadBlobRange() throws IOException {
        expectThrows(UnsupportedOperationException.class, () -> getBlobContainer().readBlob("test", 0, 12));
    }
}
