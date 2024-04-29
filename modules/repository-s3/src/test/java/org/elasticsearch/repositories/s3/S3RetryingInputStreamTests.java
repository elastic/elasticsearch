/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.repositories.blobstore.RequestedRangeNotSatisfiedException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;

import static org.elasticsearch.repositories.blobstore.BlobStoreTestUtil.randomPurpose;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class S3RetryingInputStreamTests extends ESTestCase {

    public void testInputStreamFullyConsumed() throws IOException {
        final byte[] expectedBytes = randomByteArrayOfLength(randomIntBetween(1, 512));

        final S3RetryingInputStream stream = createInputStream(expectedBytes, null, null);
        Streams.consumeFully(stream);

        assertThat(stream.isEof(), is(true));
        assertThat(stream.isAborted(), is(false));
    }

    public void testInputStreamIsAborted() throws IOException {
        final byte[] expectedBytes = randomByteArrayOfLength(randomIntBetween(10, 512));
        final byte[] actualBytes = new byte[randomIntBetween(1, Math.max(1, expectedBytes.length - 1))];

        final S3RetryingInputStream stream = createInputStream(expectedBytes, null, null);
        stream.read(actualBytes);
        stream.close();

        assertArrayEquals(Arrays.copyOf(expectedBytes, actualBytes.length), actualBytes);
        assertThat(stream.isEof(), is(false));
        assertThat(stream.isAborted(), is(true));
    }

    public void testRangeInputStreamFullyConsumed() throws IOException {
        final byte[] bytes = randomByteArrayOfLength(randomIntBetween(1, 512));
        final int position = randomIntBetween(0, bytes.length - 1);
        final int length = randomIntBetween(1, bytes.length - position);

        final S3RetryingInputStream stream = createInputStream(bytes, position, length);
        Streams.consumeFully(stream);

        assertThat(stream.isEof(), is(true));
        assertThat(stream.isAborted(), is(false));
    }

    public void testRangeInputStreamIsAborted() throws IOException {
        final byte[] expectedBytes = randomByteArrayOfLength(randomIntBetween(10, 512));
        final byte[] actualBytes = new byte[randomIntBetween(1, Math.max(1, expectedBytes.length - 1))];

        final int length = randomIntBetween(actualBytes.length + 1, expectedBytes.length);
        final int position = randomIntBetween(0, Math.max(1, expectedBytes.length - length));

        final S3RetryingInputStream stream = createInputStream(expectedBytes, position, length);
        stream.read(actualBytes);
        stream.close();

        assertArrayEquals(Arrays.copyOfRange(expectedBytes, position, position + actualBytes.length), actualBytes);
        assertThat(stream.isEof(), is(false));
        assertThat(stream.isAborted(), is(true));
    }

    public void testReadAfterBlobLengthThrowsRequestedRangeNotSatisfiedException() throws IOException {
        final byte[] bytes = randomByteArrayOfLength(randomIntBetween(1, 512));
        {
            final int position = bytes.length + randomIntBetween(0, 100);
            final int length = randomIntBetween(1, 100);
            var exception = expectThrows(RequestedRangeNotSatisfiedException.class, () -> {
                try (var ignored = createInputStream(bytes, position, length)) {
                    fail();
                }
            });
            assertThat(exception.getResource(), equalTo("_blob"));
            assertThat(exception.getPosition(), equalTo((long) position));
            assertThat(exception.getLength(), equalTo((long) length));
            assertThat(
                exception.getMessage(),
                startsWith("Requested range [position=" + position + ", length=" + length + "] cannot be satisfied for [_blob]")
            );
        }
        {
            int position = randomIntBetween(0, Math.max(0, bytes.length - 1));
            int maxLength = bytes.length - position;
            int length = randomIntBetween(maxLength + 1, Integer.MAX_VALUE - 1);
            try (var stream = createInputStream(bytes, position, length)) {
                assertThat(Streams.consumeFully(stream), equalTo((long) maxLength));
            }
        }
    }

    private S3RetryingInputStream createInputStream(final byte[] data, @Nullable final Integer position, @Nullable final Integer length)
        throws IOException {
        final AmazonS3 client = mock(AmazonS3.class);
        final AmazonS3Reference clientReference = mock(AmazonS3Reference.class);
        when(clientReference.client()).thenReturn(client);
        final S3BlobStore blobStore = mock(S3BlobStore.class);
        when(blobStore.clientReference()).thenReturn(clientReference);

        if (position != null && length != null) {
            if (data.length <= position) {
                var amazonS3Exception = new AmazonS3Exception("test");
                amazonS3Exception.setStatusCode(RestStatus.REQUESTED_RANGE_NOT_SATISFIED.getStatus());
                when(client.getObject(any(GetObjectRequest.class))).thenThrow(amazonS3Exception);
                return new S3RetryingInputStream(randomPurpose(), blobStore, "_blob", position, Math.addExact(position, length - 1));
            }

            final S3Object s3Object = new S3Object();
            s3Object.getObjectMetadata().setContentLength(length);
            s3Object.setObjectContent(new S3ObjectInputStream(new ByteArrayInputStream(data, position, length), new HttpGet()));
            when(client.getObject(any(GetObjectRequest.class))).thenReturn(s3Object);
            return new S3RetryingInputStream(randomPurpose(), blobStore, "_blob", position, Math.addExact(position, length - 1));
        }

        final S3Object s3Object = new S3Object();
        s3Object.getObjectMetadata().setContentLength(data.length);
        s3Object.setObjectContent(new S3ObjectInputStream(new ByteArrayInputStream(data), new HttpGet()));
        when(client.getObject(any(GetObjectRequest.class))).thenReturn(s3Object);
        return new S3RetryingInputStream(randomPurpose(), blobStore, "_blob");
    }
}
