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

import com.sun.net.httpserver.HttpHandler;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.rest.RestStatus;

import java.io.InputStream;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.repositories.blobstore.BlobStoreTestUtil.randomRetryingPurpose;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

/**
 * This class tests how a {@link AzureBlobContainer} and its underlying SDK client are retrying requests when reading or writing blobs.
 */
@SuppressForbidden(reason = "use a http server")
public class AzureBlobContainerRetriesTests extends AbstractAzureServerTestCase {

    public void testRetryFromSecondaryLocationPolicies() throws Exception {
        final int maxRetries = randomIntBetween(1, 5);
        final AtomicInteger failedHeadCalls = new AtomicInteger();
        final AtomicInteger failedGetCalls = new AtomicInteger();
        final byte[] bytes = randomBlobContent();

        HttpHandler failingHandler = exchange -> {
            try {
                Streams.readFully(exchange.getRequestBody());
                if ("HEAD".equals(exchange.getRequestMethod())) {
                    failedHeadCalls.incrementAndGet();
                    AzureHttpHandler.sendError(exchange, randomFrom(RestStatus.INTERNAL_SERVER_ERROR, RestStatus.SERVICE_UNAVAILABLE));
                } else if ("GET".equals(exchange.getRequestMethod())) {
                    failedGetCalls.incrementAndGet();
                    AzureHttpHandler.sendError(exchange, randomFrom(RestStatus.INTERNAL_SERVER_ERROR, RestStatus.SERVICE_UNAVAILABLE));
                }
            } finally {
                exchange.close();
            }
        };

        HttpHandler workingHandler = exchange -> {
            try {
                Streams.readFully(exchange.getRequestBody());
                if ("HEAD".equals(exchange.getRequestMethod())) {
                    exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
                    exchange.getResponseHeaders().add("x-ms-blob-content-length", String.valueOf(bytes.length));
                    exchange.getResponseHeaders().add("Content-Length", String.valueOf(bytes.length));
                    exchange.getResponseHeaders().add("x-ms-blob-type", "blockblob");
                    exchange.sendResponseHeaders(RestStatus.OK.getStatus(), -1);
                } else if ("GET".equals(exchange.getRequestMethod())) {
                    final int rangeStart = getRangeStart(exchange);
                    assertThat(rangeStart, lessThan(bytes.length));
                    final int length = bytes.length - rangeStart;
                    exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
                    exchange.getResponseHeaders().add("x-ms-blob-content-length", String.valueOf(length));
                    exchange.getResponseHeaders().add("Content-Length", String.valueOf(length));
                    exchange.getResponseHeaders().add("x-ms-blob-type", "blockblob");
                    exchange.getResponseHeaders().add("ETag", UUIDs.base64UUID());
                    exchange.sendResponseHeaders(RestStatus.OK.getStatus(), length);
                    exchange.getResponseBody().write(bytes, rangeStart, length);
                }
            } finally {
                exchange.close();
            }
        };
        LocationMode locationMode = randomFrom(LocationMode.PRIMARY_THEN_SECONDARY, LocationMode.SECONDARY_THEN_PRIMARY);

        String secondaryHost = null;
        String blobPath = "/account/container/read_blob_from_secondary";
        if (locationMode == LocationMode.PRIMARY_THEN_SECONDARY) {
            httpServer.createContext(blobPath, failingHandler);
            secondaryHttpServer.createContext(blobPath, workingHandler);
            // The SDK doesn't work well with secondary host endpoints that contain
            // a path, that's the reason why we sould provide just the host + port;
            secondaryHost = getEndpointForServer(secondaryHttpServer, "account");
        } else if (locationMode == LocationMode.SECONDARY_THEN_PRIMARY) {
            secondaryHttpServer.createContext(blobPath, failingHandler);
            httpServer.createContext(blobPath, workingHandler);
            secondaryHost = getEndpointForServer(httpServer, "account");
        }

        final BlobContainer blobContainer = createBlobContainer(maxRetries, secondaryHost, locationMode);
        try (InputStream inputStream = blobContainer.readBlob(randomRetryingPurpose(), "read_blob_from_secondary")) {
            assertArrayEquals(bytes, BytesReference.toBytes(Streams.readFully(inputStream)));

            // It does round robin, first tries on the primary, then on the secondary
            assertThat(failedHeadCalls.get(), equalTo(1));
            assertThat(failedGetCalls.get(), equalTo(1));
        }
    }
}
