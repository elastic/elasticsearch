/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.gcs;

import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.testing.http.HttpTesting;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;

import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.repositories.blobstore.RequestedRangeNotSatisfiedException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Locale;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GoogleCloudStorageRetryingInputStreamTests extends ESTestCase {

    private static final String BUCKET_NAME = "test-bucket";
    private static final String BLOB_NAME = "test-blob";

    private final BlobId blobId = BlobId.of(BUCKET_NAME, BLOB_NAME);

    private com.google.api.services.storage.Storage storageRpc;
    private com.google.cloud.storage.Storage storage;
    private com.google.api.services.storage.Storage.Objects.Get get;
    private MeteredStorage meteredStorage;

    @Before
    public void init() throws IOException {
        storageRpc = mock(com.google.api.services.storage.Storage.class);
        com.google.api.services.storage.Storage.Objects objects = mock(com.google.api.services.storage.Storage.Objects.class);
        when(storageRpc.objects()).thenReturn(objects);

        get = mock(com.google.api.services.storage.Storage.Objects.Get.class);
        when(objects.get(BUCKET_NAME, BLOB_NAME)).thenReturn(get);

        storage = mock(com.google.cloud.storage.Storage.class);
        when(storage.getOptions()).thenReturn(
            StorageOptions.newBuilder()
                .setProjectId("ignore")
                .setRetrySettings(RetrySettings.newBuilder().setMaxAttempts(randomIntBetween(1, 3)).build())
                .build()
        );
        meteredStorage = new MeteredStorage(storage, storageRpc, new GcsRepositoryStatsCollector());
    }

    public void testReadWithinBlobLength() throws IOException {
        byte[] bytes = randomByteArrayOfLength(randomIntBetween(1, 512));
        int position = randomIntBetween(0, Math.max(0, bytes.length - 1));
        int maxLength = bytes.length - position;    // max length to read if length param exceeds buffer size
        int length = randomIntBetween(0, Integer.MAX_VALUE - 1);

        GoogleCloudStorageRetryingInputStream stream;
        boolean readWithExactPositionAndLength = randomBoolean();
        if (readWithExactPositionAndLength) {
            stream = createRetryingInputStream(bytes, position, length);
        } else {
            stream = createRetryingInputStream(bytes);
        }
        try (stream) {
            var out = new ByteArrayOutputStream();
            var readLength = org.elasticsearch.core.Streams.copy(stream, out);
            if (readWithExactPositionAndLength) {
                assertThat(readLength, equalTo((long) Math.min(length, maxLength)));
                assertArrayEquals(out.toByteArray(), Arrays.copyOfRange(bytes, position, position + Math.min(length, maxLength)));
            } else {
                assertThat(readLength, equalTo((long) bytes.length));
                assertArrayEquals(out.toByteArray(), bytes);
            }
        }
    }

    public void testReadBeyondBlobLengthThrowsRequestedRangeNotSatisfiedException() {
        byte[] bytes = randomByteArrayOfLength(randomIntBetween(1, 512));
        int position = bytes.length + randomIntBetween(0, 100);
        int length = randomIntBetween(1, 100);
        var exception = expectThrows(RequestedRangeNotSatisfiedException.class, () -> {
            try (var ignored = createRetryingInputStream(bytes, position, length)) {
                fail();
            }
        });
        assertThat(exception.getResource(), equalTo(BLOB_NAME));
        assertThat(exception.getPosition(), equalTo((long) position));
        assertThat(exception.getLength(), equalTo((long) length));
        assertThat(
            exception.getMessage(),
            equalTo(
                String.format(
                    Locale.ROOT,
                    "Requested range [position=%d, length=%d] cannot be satisfied for [%s]",
                    position,
                    length,
                    BLOB_NAME
                )
            )
        );
        assertThat(exception.getCause(), instanceOf(StorageException.class));
    }

    private GoogleCloudStorageRetryingInputStream createRetryingInputStream(byte[] data) throws IOException {

        HttpTransport transport = getMockHttpTransport(data, 0, data.length);
        HttpRequest httpRequest = transport.createRequestFactory().buildGetRequest(HttpTesting.SIMPLE_GENERIC_URL);
        HttpResponse httpResponse = httpRequest.execute();
        when(get.executeMedia()).thenReturn(httpResponse);

        return new GoogleCloudStorageRetryingInputStream(OperationPurpose.SNAPSHOT_DATA, meteredStorage, blobId);
    }

    private GoogleCloudStorageRetryingInputStream createRetryingInputStream(byte[] data, int position, int length) throws IOException {

        if (position >= data.length) {
            when(get.executeMedia()).thenThrow(
                new StorageException(RestStatus.REQUESTED_RANGE_NOT_SATISFIED.getStatus(), "Test range not satisfied")
            );
        } else {
            HttpTransport transport = getMockHttpTransport(data, position, length);
            HttpRequest httpRequest = transport.createRequestFactory().buildGetRequest(HttpTesting.SIMPLE_GENERIC_URL);
            HttpResponse httpResponse = httpRequest.execute();
            when(get.executeMedia()).thenReturn(httpResponse);
        }

        return new GoogleCloudStorageRetryingInputStream(
            OperationPurpose.SNAPSHOT_DATA,
            meteredStorage,
            blobId,
            position,
            position + length - 1
        );
    }

    private static HttpTransport getMockHttpTransport(byte[] data, int position, int length) {
        InputStream content = new ByteArrayInputStream(data, position, length);
        long contentLength = position + length - 1;
        HttpTransport transport = new MockHttpTransport() {
            @Override
            public LowLevelHttpRequest buildRequest(String method, String url) throws IOException {
                return new MockLowLevelHttpRequest() {
                    @Override
                    public LowLevelHttpResponse execute() throws IOException {
                        MockLowLevelHttpResponse result = new MockLowLevelHttpResponse();
                        result.setContent(content);
                        result.setContentLength(contentLength);
                        result.setContentType("application/octet-stream");
                        result.addHeader("x-goog-generation", String.valueOf(randomNonNegativeInt()));
                        result.setStatusCode(RestStatus.OK.getStatus());
                        return result;
                    }
                };
            }
        };
        return transport;
    }
}
