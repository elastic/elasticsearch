/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.azure;

import com.sun.net.httpserver.HttpExchange;

import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;

import java.io.InputStream;
import java.util.Base64;

import static org.elasticsearch.repositories.blobstore.BlobStoreTestUtil.randomFiniteRetryingPurpose;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.lessThan;

@SuppressForbidden(reason = "use a http server")
public class AzureBlobContainerTests extends AbstractAzureServerTestCase {

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
