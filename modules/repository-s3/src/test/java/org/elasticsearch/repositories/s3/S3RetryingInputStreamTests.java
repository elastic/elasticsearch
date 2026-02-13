/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.s3;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.metrics.MetricPublisher;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.repositories.blobstore.RequestedRangeNotSatisfiedException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.function.BiConsumer;
import java.util.function.ToLongFunction;

import static org.elasticsearch.repositories.blobstore.BlobStoreTestUtil.randomPurpose;
import static org.elasticsearch.repositories.blobstore.BlobStoreTestUtil.randomRetryingPurpose;
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
        assertEquals(actualBytes.length, stream.read(actualBytes));
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
        assertEquals(actualBytes.length, stream.read(actualBytes));
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

    public void testContentRangeValidation() throws IOException {
        final byte[] bytes = randomByteArrayOfLength(between(101, 200));
        final int position = between(0, 100);
        final int length = between(1, 100);
        try (var stream = createInputStream(bytes, position, length)) {

            final ToLongFunction<String> lengthSupplier = contentRangeHeader -> stream.tryGetStreamLength(
                GetObjectResponse.builder().contentRange(contentRangeHeader).build()
            );

            final var fakeLength = between(1, length);
            assertEquals(fakeLength, lengthSupplier.applyAsLong("bytes " + position + "-" + (position + fakeLength - 1) + "/*"));
            assertEquals(fakeLength, stream.tryGetStreamLength(GetObjectResponse.builder().contentLength((long) fakeLength).build()));

            final BiConsumer<String, String> failureMessageAsserter = (contentRangeHeader, expectedMessage) -> assertEquals(
                expectedMessage,
                expectThrows(IllegalArgumentException.class, () -> lengthSupplier.applyAsLong(contentRangeHeader)).getMessage()
            );

            failureMessageAsserter.accept("invalid", "unexpected Content-range header [invalid], should have started with [bytes ]");
            failureMessageAsserter.accept("bytes invalid", "could not parse Content-range header [bytes invalid], missing hyphen");
            failureMessageAsserter.accept("bytes 0-1", "could not parse Content-range header [bytes 0-1], missing slash");

            final var badStartPos = randomValueOtherThan(position, () -> between(0, 100));
            final var badStartHeader = Strings.format("bytes %d-%d/*", badStartPos, between(badStartPos, 200));
            failureMessageAsserter.accept(
                badStartHeader,
                "unexpected Content-range header [" + badStartHeader + "], should have started at " + position
            );

            final var badEndPos = between(position + length + 1, 201);
            final var badEndHeader = Strings.format("bytes %d-%d/*", position, badEndPos);
            failureMessageAsserter.accept(
                badEndHeader,
                "unexpected Content-range header [" + badEndHeader + "], should have ended no later than " + (position + length - 1)
            );
        }
    }

    public void testRetriesAreTerminatedWhenOpenThrowsNonSdkException() throws IOException {
        final int blobLength = randomIntBetween(500, 1000);
        final S3BlobStore blobStore = mock(S3BlobStore.class);
        final AmazonS3Reference clientReference = mock(AmazonS3Reference.class);
        when(blobStore.clientReference()).thenReturn(clientReference);
        when(blobStore.getRepositoryMetadata()).thenReturn(new RepositoryMetadata(randomIdentifier(), randomIdentifier(), Settings.EMPTY));
        when(blobStore.getS3RepositoriesMetrics()).thenReturn(S3RepositoriesMetrics.NOOP);
        when(blobStore.getMetricPublisher(any(), any())).thenReturn(mock(MetricPublisher.class));
        when(blobStore.getMaxRetries()).thenReturn(randomIntBetween(5, 1000));

        final var fatalException = randomBoolean() ? new IllegalStateException("test") : new RuntimeException("test");
        final OperationPurpose purpose;
        if (randomBoolean()) {
            final S3Client failImmediatelyS3Client = mock(S3Client.class);
            when(failImmediatelyS3Client.getObject(any(GetObjectRequest.class))).thenThrow(fatalException);
            when(clientReference.client()).thenReturn(failImmediatelyS3Client);
            purpose = randomPurpose();
        } else {
            final S3Client failOnFirstRetryS3Client = mock(S3Client.class);
            when(failOnFirstRetryS3Client.getObject(any(GetObjectRequest.class))).thenReturn(
                new ResponseInputStream<>(
                    GetObjectResponse.builder().contentLength((long) blobLength).build(),
                    new FailDuringReadInputStream(blobLength)
                )
            ).thenThrow(fatalException);
            when(clientReference.client()).thenReturn(failOnFirstRetryS3Client);
            // OperationPurpose.INDICES will retry forever, so we want to exercise that especially,
            // but the behavior should be consistent for all purposes
            purpose = randomBoolean() ? OperationPurpose.INDICES : randomRetryingPurpose();
        }
        try {
            final S3RetryingInputStream stream = new S3RetryingInputStream(purpose, blobStore, "_blob");
            Streams.consumeFully(stream);
            fail("Exception wasn't thrown?!");
        } catch (RuntimeException e) {
            assertSame(fatalException, e);
        }
    }

    private static class FailDuringReadInputStream extends InputStream {

        private final byte[] contents;
        private final int failurePosition;
        private int position = 0;

        FailDuringReadInputStream(int totalLength) {
            contents = randomByteArrayOfLength(totalLength);
            failurePosition = randomIntBetween(0, totalLength - 1);
        }

        @Override
        public int read() throws IOException {
            if (position == failurePosition) {
                throw new IOException("this should be retryable");
            }
            return contents[position++] & 0xFF;
        }
    }

    /**
     * Creates a mock BlobStore that returns a mock S3Client, configured to supply a #getObject response. The blob store is then wrapped in
     * a {@link S3RetryingInputStream}.
     *
     * @param data The data to stream.
     * @param position The position at which to start reading from the stream.
     * @param length How much to read from the data stream starting at {@code position}
     * @return A {@link S3RetryingInputStream} that reads from the data stream.
     */
    private S3RetryingInputStream createInputStream(final byte[] data, @Nullable final Integer position, @Nullable final Integer length)
        throws IOException {
        final S3Client client = mock(S3Client.class);
        final AmazonS3Reference clientReference = mock(AmazonS3Reference.class);
        when(clientReference.client()).thenReturn(client);
        final S3BlobStore blobStore = mock(S3BlobStore.class);
        when(blobStore.clientReference()).thenReturn(clientReference);
        final MetricPublisher metricPublisher = mock(MetricPublisher.class);
        when(blobStore.getMetricPublisher(any(S3BlobStore.Operation.class), any(OperationPurpose.class))).thenReturn(metricPublisher);

        if (position != null && length != null) {
            if (data.length <= position) {
                var s3Exception = S3Exception.builder().message("test");
                s3Exception.statusCode(RestStatus.REQUESTED_RANGE_NOT_SATISFIED.getStatus());
                when(client.getObject(any(GetObjectRequest.class))).thenThrow(s3Exception.build());
                return new S3RetryingInputStream(randomPurpose(), blobStore, "_blob", position, Math.addExact(position, length - 1));
            }

            ResponseInputStream<GetObjectResponse> objectResponse = new ResponseInputStream<>(
                GetObjectResponse.builder().contentLength(length.longValue()).build(),
                new ByteArrayInputStream(data, position, length)
            );
            when(client.getObject(any(GetObjectRequest.class))).thenReturn(objectResponse);
            return new S3RetryingInputStream(randomPurpose(), blobStore, "_blob", position, Math.addExact(position, length - 1));
        }

        ResponseInputStream<GetObjectResponse> objectResponse = new ResponseInputStream<>(
            GetObjectResponse.builder().contentLength(Long.valueOf(data.length)).build(),
            new ByteArrayInputStream(data)
        );
        when(client.getObject(any(GetObjectRequest.class))).thenReturn(objectResponse);
        return new S3RetryingInputStream(randomPurpose(), blobStore, "_blob");
    }
}
