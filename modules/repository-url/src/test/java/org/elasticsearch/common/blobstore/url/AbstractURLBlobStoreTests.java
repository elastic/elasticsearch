/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.blobstore.url;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;

import static org.elasticsearch.repositories.blobstore.BlobStoreTestUtil.randomPurpose;
import static org.hamcrest.core.IsEqual.equalTo;

public abstract class AbstractURLBlobStoreTests extends ESTestCase {
    abstract BytesArray getOriginalData();

    abstract BlobContainer getBlobContainer();

    abstract String getBlobName();

    public void testURLBlobStoreCanReadBlob() throws IOException {
        BytesArray data = getOriginalData();
        String blobName = getBlobName();
        BlobContainer container = getBlobContainer();
        try (InputStream stream = container.readBlob(randomPurpose(), blobName)) {
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
        try (InputStream stream = container.readBlob(randomPurpose(), blobName, position, length)) {
            BytesReference bytesRead = Streams.readFully(stream);
            assertThat(data.slice(position, length), equalTo(bytesRead));
        }
    }

    public void testNoBlobFound() throws IOException {
        BlobContainer container = getBlobContainer();
        String incorrectBlobName = UUIDs.base64UUID();
        try (InputStream ignored = container.readBlob(randomPurpose(), incorrectBlobName)) {
            ignored.read();
            fail("Should have thrown NoSuchFileException exception");
        } catch (NoSuchFileException e) {
            assertEquals(Strings.format("blob object [%s] not found", incorrectBlobName), e.getMessage());
        }
    }
}
