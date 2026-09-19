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

import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.rest.RestStatus;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.elasticsearch.repositories.blobstore.BlobStoreTestUtil.randomPurpose;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

/**
 * Checks that uploads declare a {@code Content-MD5} the service can hold them to.
 *
 * <p>A commit blob that is corrupted between being read and reaching the wire is otherwise stored and acknowledged,
 * and only surfaces days later as a Lucene checksum failure on a shard that can no longer be recovered without
 * losing the writes that followed. Azure verifies this header against the body it receives, so sending it turns that
 * silent corruption into a failed request the existing retry machinery can handle.
 */
@SuppressForbidden(reason = "use a http server")
public class AzureUploadContentMd5Tests extends AbstractAzureServerTestCase {

    /** The {@code Content-MD5} of every upload body the fixture saw, paired with the digest of the body itself. */
    private record Upload(String declaredMd5, String actualMd5, int length) {}

    public void testSinglePartUploadDeclaresContentMd5() throws Exception {
        final List<Upload> uploads = new CopyOnWriteArrayList<>();
        final BlobContainer container = createBlobContainer(3);
        recordUploads(uploads);

        // below MAX_SINGLE_PART_UPLOAD_SIZE (1MB in this harness), so this is one Put Blob
        final byte[] bytes = randomByteArrayOfLength(between(1024, 512 * 1024));
        container.writeBlobAtomic(randomPurpose(), "single", bytes.length, providerFor(bytes), false, Runnable::run);

        assertThat(uploads, hasSize(1));
        assertDeclaredMatchesBody(uploads);
    }

    public void testMultiPartUploadDeclaresContentMd5PerPart() throws Exception {
        final List<Upload> uploads = new CopyOnWriteArrayList<>();
        final BlobContainer container = createBlobContainer(3);
        recordUploads(uploads);

        // above the 1MB block size, so this stages several parts and each carries its own digest
        final byte[] bytes = randomByteArrayOfLength(Math.toIntExact(ByteSizeValue.of(3, ByteSizeUnit.MB).getBytes()));
        container.writeBlobAtomic(randomPurpose(), "multi", bytes.length, providerFor(bytes), false, Runnable::run);

        assertThat(uploads.size(), greaterThan(1));
        assertDeclaredMatchesBody(uploads);
    }

    /**
     * A body that does not match its declared digest must fail the write rather than be stored. This stands in for
     * the corruption itself, which cannot be provoked on demand: what matters is that the rejection reaches the
     * caller instead of the upload reporting success.
     */
    public void testMismatchedContentMd5FailsTheWrite() throws Exception {
        final BlobContainer container = createBlobContainer(0);
        httpServer.createContext("/account/container/mismatch", exchange -> {
            try {
                Streams.readFully(exchange.getRequestBody());
                AzureHttpHandler.sendError(
                    exchange,
                    RestStatus.BAD_REQUEST,
                    "Md5Mismatch",
                    "The MD5 value specified in the request did not match with the MD5 value calculated by the server."
                );
            } finally {
                exchange.close();
            }
        });

        final byte[] bytes = randomByteArrayOfLength(between(1024, 8192));
        final IOException e = expectThrows(
            IOException.class,
            () -> container.writeBlobAtomic(randomPurpose(), "mismatch", bytes.length, providerFor(bytes), false, Runnable::run)
        );
        assertThat(e.getMessage(), containsString("Unable to write blob mismatch"));
    }

    private static BlobContainer.BlobMultiPartInputStreamProvider providerFor(byte[] bytes) {
        return (offset, length) -> new ByteArrayInputStream(bytes, Math.toIntExact(offset), Math.toIntExact(length));
    }

    private static void assertDeclaredMatchesBody(List<Upload> uploads) {
        assertThat("every upload should declare a Content-MD5", uploads.stream().map(Upload::declaredMd5).toList(), everyItem(not("")));
        for (Upload upload : uploads) {
            assertEquals(
                "Content-MD5 does not describe the body of a " + upload.length() + " byte upload",
                upload.actualMd5(),
                upload.declaredMd5()
            );
        }
    }

    /** Accepts every upload, recording the digest it declared alongside the digest of what actually arrived. */
    private void recordUploads(List<Upload> uploads) {
        httpServer.createContext("/account/container/", exchange -> {
            try {
                final BytesReference body = Streams.readFully(exchange.getRequestBody());
                if ("PUT".equals(exchange.getRequestMethod())
                    && (exchange.getRequestURI().getQuery() == null || exchange.getRequestURI().getQuery().contains("blockid="))) {
                    final String declared = exchange.getRequestHeaders().getFirst("Content-MD5");
                    uploads.add(
                        new Upload(
                            declared == null ? "" : declared,
                            Base64.getEncoder().encodeToString(MessageDigests.digest(body, MessageDigests.md5())),
                            body.length()
                        )
                    );
                }
                exchange.getResponseHeaders().add("x-ms-request-server-encrypted", "false");
                exchange.sendResponseHeaders(RestStatus.CREATED.getStatus(), -1);
            } finally {
                exchange.close();
            }
        });
    }
}
