/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.azure;

import fixture.azure.AzureHttpHandler;
import fixture.azure.MockAzureBlobStore;

import com.sun.net.httpserver.HttpExchange;

import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Base64;

import static org.elasticsearch.repositories.blobstore.BlobStoreTestUtil.randomFiniteRetryingPurpose;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.lessThan;

@SuppressForbidden(reason = "use a http server")
public class AzureBlobContainerTests extends AbstractAzureServerTestCase {

    private AzureHttpHandler azureHttpHandler;

    @Before
    public void configureAzureHandler() {
        azureHttpHandler = new AzureHttpHandler(ACCOUNT, CONTAINER, null, MockAzureBlobStore.LeaseExpiryPredicate.NEVER_EXPIRE);
        httpServer.createContext("/", azureHttpHandler);
    }

    public void testDataAccessTierSentOnSingleBlobUpload() throws IOException {
        final String tierInput = randomFrom("hot", "Hot", "HOT");
        final AzureBlobContainer container = asInstanceOf(AzureBlobContainer.class, builder().withDataAccessTier(tierInput).build());
        final String blobName = randomIdentifier();

        container.getBlobStore()
            .writeBlob(
                OperationPurpose.SNAPSHOT_DATA,
                blobName,
                BytesReference.fromByteBuffer(ByteBuffer.wrap(randomByteArrayOfLength(128))),
                false
            );

        assertEquals("Hot", azureHttpHandler.getMockBlobStore().getBlob(blobName, null).accessTier());
    }

    public void testMetadataAccessTierSentOnSingleBlobUpload() throws IOException {
        final AzureBlobContainer container = asInstanceOf(AzureBlobContainer.class, builder().withMetadataAccessTier("cool").build());
        final String blobName = randomIdentifier();

        container.getBlobStore()
            .writeBlob(
                OperationPurpose.SNAPSHOT_METADATA,
                blobName,
                BytesReference.fromByteBuffer(ByteBuffer.wrap(randomByteArrayOfLength(128))),
                false
            );

        assertEquals("Cool", azureHttpHandler.getMockBlobStore().getBlob(blobName, null).accessTier());
    }

    public void testNoTierSentWhenNotConfigured() throws IOException {
        final AzureBlobContainer container = asInstanceOf(AzureBlobContainer.class, builder().build());
        final String blobName = randomIdentifier();

        container.getBlobStore()
            .writeBlob(
                OperationPurpose.SNAPSHOT_DATA,
                blobName,
                BytesReference.fromByteBuffer(ByteBuffer.wrap(randomByteArrayOfLength(128))),
                false
            );

        assertNull(azureHttpHandler.getMockBlobStore().getBlob(blobName, null).accessTier());
    }

    public void testNoTierSentForNonSnapshotPurpose() throws IOException {
        final AzureBlobContainer container = asInstanceOf(
            AzureBlobContainer.class,
            builder().withDataAccessTier("hot").withMetadataAccessTier("cool").build()
        );
        final String blobName = randomIdentifier();

        container.getBlobStore()
            .writeBlob(
                OperationPurpose.CLUSTER_STATE,
                blobName,
                BytesReference.fromByteBuffer(ByteBuffer.wrap(randomByteArrayOfLength(128))),
                false
            );

        assertNull(azureHttpHandler.getMockBlobStore().getBlob(blobName, null).accessTier());
    }

    public void testDataAccessTierSentOnMultipartUpload() throws IOException {
        final AzureBlobContainer container = asInstanceOf(AzureBlobContainer.class, builder().withDataAccessTier("cold").build());
        final String blobName = randomIdentifier();

        // Write more than the 1MB threshold configured in AbstractAzureServerTestCase to trigger multipart upload
        final int blobSize = (int) (container.getBlobStore().getUploadBlockSize() + 1);
        final byte[] data = randomByteArrayOfLength(blobSize);

        container.getBlobStore().writeBlob(OperationPurpose.SNAPSHOT_DATA, blobName, new ByteArrayInputStream(data), blobSize, false);

        assertEquals("Cold", azureHttpHandler.getMockBlobStore().getBlob(blobName, null).accessTier());
    }

    public void testMetadataAccessTierSentOnWriteMetadataBlobMultipartUpload() throws IOException {
        final AzureBlobContainer container = asInstanceOf(AzureBlobContainer.class, builder().withMetadataAccessTier("cool").build());
        final String blobName = randomIdentifier();

        // Write more than the 1MB threshold to flush at least one block, triggering the BlockBlobCommitBlockListOptions path
        final int blobSize = (int) (container.getBlobStore().getUploadBlockSize() + 1);
        final byte[] data = randomByteArrayOfLength(blobSize);

        container.writeMetadataBlob(OperationPurpose.SNAPSHOT_METADATA, blobName, false, false, out -> out.write(data));

        assertEquals("Cool", azureHttpHandler.getMockBlobStore().getBlob(blobName, null).accessTier());
    }

    public void testDataAccessTierSentOnWriteBlobAtomicMultipartUpload() throws IOException {
        final AzureBlobContainer container = asInstanceOf(AzureBlobContainer.class, builder().withDataAccessTier("cold").build());
        final String blobName = randomIdentifier();

        // Write more than the single-part threshold to trigger the concurrent multipart path in writeBlobAtomic
        final int blobSize = (int) (container.getBlobStore().getLargeBlobThresholdInBytes() + 1);
        final byte[] data = randomByteArrayOfLength(blobSize);

        container.writeBlobAtomic(
            OperationPurpose.SNAPSHOT_DATA,
            blobName,
            blobSize,
            (offset, length) -> new ByteArrayInputStream(data, Math.toIntExact(offset), Math.toIntExact(length)),
            false
        );

        assertEquals("Cold", azureHttpHandler.getMockBlobStore().getBlob(blobName, null).accessTier());
    }

    public void testDataAccessTierSentOnCopyBlob() throws IOException {
        final AzureBlobContainer container = asInstanceOf(AzureBlobContainer.class, builder().withDataAccessTier("hot").build());
        final String sourceBlobName = randomIdentifier();
        final String destBlobName = randomIdentifier();
        final byte[] data = randomByteArrayOfLength(128);

        // Write source blob with a non-snapshot purpose so it has no access tier
        container.getBlobStore()
            .writeBlob(OperationPurpose.CLUSTER_STATE, sourceBlobName, BytesReference.fromByteBuffer(ByteBuffer.wrap(data)), false);
        assertNull(azureHttpHandler.getMockBlobStore().getBlob(sourceBlobName, null).accessTier());

        // Copy with snapshot data purpose — the destination should receive the configured data access tier
        container.copyBlob(OperationPurpose.SNAPSHOT_DATA, container, sourceBlobName, destBlobName, data.length);

        assertEquals("Hot", azureHttpHandler.getMockBlobStore().getBlob(destBlobName, null).accessTier());
    }

    public void testCanConfigureReadTimeout() {
        final byte[] bytes = randomBlobContent();
        httpServer.createContext("/account/container/read_blob_read_timeout", exchange -> {
            logger.info("Received request: {} {}", exchange.getRequestMethod(), exchange.getRequestURI());
            Streams.readFully(exchange.getRequestBody());
            if ("HEAD".equals(exchange.getRequestMethod())) {
                sendBlobHeaders(exchange, bytes);
                exchange.sendResponseHeaders(RestStatus.OK.getStatus(), -1);
                exchange.close();
            } else if ("GET".equals(exchange.getRequestMethod())) {
                sendBlobHeaders(exchange, bytes);
                exchange.sendResponseHeaders(RestStatus.OK.getStatus(), bytes.length);
                // Send the response headers back then stop (this is required to trigger the read timeout)
                exchange.getResponseBody().flush();
            }
        });

        /*
         * The read timeout should be reflected in the timeout message
         */
        {
            final var tryTimeout = TimeValue.timeValueSeconds(60);
            final var readTimeoutMillis = randomLongBetween(100, 1000);
            final BlobContainer blobContainer = builder().withMaxRetries(0)
                .withTryTimeout(tryTimeout)
                .withReadTimeout(TimeValue.timeValueMillis(readTimeoutMillis))
                .build();
            final long startTimeMillis = System.currentTimeMillis();
            final RuntimeException readBlobException = assertThrows(RuntimeException.class, () -> {
                try (InputStream inputStream = blobContainer.readBlob(randomFiniteRetryingPurpose(), "read_blob_read_timeout")) {
                    assertArrayEquals(bytes, BytesReference.toBytes(Streams.readFully(inputStream)));
                }
            });
            assertThat(
                readBlobException.getMessage(),
                containsString("Channel read timed out after " + readTimeoutMillis + " milliseconds")
            );
            final long elapsedTimeMillis = System.currentTimeMillis() - startTimeMillis;
            assertThat(elapsedTimeMillis, lessThan(tryTimeout.millis()));
        }
    }

    protected void sendBlobHeaders(HttpExchange exchange, byte[] blobContents) {
        exchange.getResponseHeaders().add("x-ms-blob-content-length", String.valueOf(blobContents.length));
        exchange.getResponseHeaders().add("Content-Length", String.valueOf(blobContents.length));
        exchange.getResponseHeaders().add("x-ms-blob-type", "blockblob");
        exchange.getResponseHeaders().add("ETag", eTagForContents(blobContents));
    }

    private static String eTagForContents(byte[] blobContents) {
        return Base64.getEncoder().encodeToString(MessageDigests.digest(new BytesArray(blobContents), MessageDigests.md5()));
    }
}
