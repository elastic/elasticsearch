/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.blobstore.url;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.Locale;

import static org.hamcrest.core.IsEqual.equalTo;

public abstract class AbstractURLBlobStoreTests extends ESTestCase {
    abstract BytesArray getOriginalData();

    abstract BlobContainer getBlobContainer();

    abstract String getBlobName();

    public void testURLBlobStoreCanReadBlob() throws IOException {
        BytesArray data = getOriginalData();
        String blobName = getBlobName();
        BlobContainer container = getBlobContainer();
        try (InputStream stream = container.readBlob(blobName)) {
            BytesReference bytesRead = Streams.readFully(stream);
            assertThat(data, equalTo(bytesRead));
        }
    }

    public void testURLBlobStoreCanReadBlobRange() throws IOException {
        BytesArray data = getOriginalData();
        String blobName = getBlobName();
        BlobContainer container = getBlobContainer();
        int position = randomIntBetween(0, data.length() - 1);
        int length = randomIntBetween(1, data.length() - position);
        try (InputStream stream = container.readBlob(blobName, position, length)) {
            BytesReference bytesRead = Streams.readFully(stream);
            assertThat(data.slice(position, length), equalTo(bytesRead));
        }
    }

    public void testNoBlobFound() throws IOException {
        BlobContainer container = getBlobContainer();
        String incorrectBlobName = UUIDs.base64UUID();
        try (InputStream ignored = container.readBlob(incorrectBlobName)) {
            ignored.read();
            fail("Should have thrown NoSuchFileException exception");
        } catch (NoSuchFileException e) {
            assertEquals(String.format(Locale.ROOT, "blob object [%s] not found", incorrectBlobName), e.getMessage());
        }
    }
}
