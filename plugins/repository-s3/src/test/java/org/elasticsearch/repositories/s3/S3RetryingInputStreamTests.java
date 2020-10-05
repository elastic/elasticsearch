/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.repositories.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;

import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
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

    private S3RetryingInputStream createInputStream(
        final byte[] data,
        @Nullable final Integer position,
        @Nullable final Integer length
    ) throws IOException {
        final S3Object s3Object = new S3Object();
        final AmazonS3 client = mock(AmazonS3.class);
        when(client.getObject(any(GetObjectRequest.class))).thenReturn(s3Object);
        final AmazonS3Reference clientReference = mock(AmazonS3Reference.class);
        when(clientReference.client()).thenReturn(client);
        final S3BlobStore blobStore = mock(S3BlobStore.class);
        when(blobStore.clientReference()).thenReturn(clientReference);

        if (position != null && length != null) {
            s3Object.getObjectMetadata().setContentLength(length);
            s3Object.setObjectContent(new S3ObjectInputStream(new ByteArrayInputStream(data, position, length), new HttpGet()));
            return new S3RetryingInputStream(blobStore, "_blob", position, Math.addExact(position, length - 1));
        } else {
            s3Object.getObjectMetadata().setContentLength(data.length);
            s3Object.setObjectContent(new S3ObjectInputStream(new ByteArrayInputStream(data), new HttpGet()));
            return new S3RetryingInputStream(blobStore, "_blob");
        }
    }
}
