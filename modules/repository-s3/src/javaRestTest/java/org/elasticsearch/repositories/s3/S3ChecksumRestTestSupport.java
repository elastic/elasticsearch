/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.s3;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.io.IOException;
import java.util.Locale;
import java.util.Set;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.rest.ESRestTestCase.assertOK;
import static org.elasticsearch.test.rest.ESRestTestCase.assertOKAndCreateObjectPath;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * What the tests that watch the checksums on S3 uploads have in common. They need to recognise checksum request headers, and they need
 * to force a snapshot to send multipart uploads, which means writing more data than the upload buffer holds.
 */
final class S3ChecksumRestTestSupport {

    /** The size of the buffer that decides when an upload is split into parts. */
    static final ByteSizeValue MULTIPART_BUFFER_SIZE = ByteSizeValue.ofMb(5);

    /**
     * Twice the buffer, so a shard's stored fields file is still bigger than one buffer after whatever little compression the random
     * text gets, and has to be sent in parts.
     */
    static final long DATA_SIZE_FORCING_MULTIPART_UPLOADS = 2 * MULTIPART_BUFFER_SIZE.getBytes();

    static final Settings SINGLE_SHARD_INDEX_SETTINGS = Settings.builder()
        .put("index.number_of_shards", 1)
        .put("index.number_of_replicas", 0)
        .build();

    /** Keeps the payload in the stored fields without indexing it, so the data is cheap to write but still lands in the shard files. */
    static final String UNINDEXED_PAYLOAD_MAPPING = """
        {"properties":{"payload":{"type":"object","enabled":false}}}""";

    private static final int DOCUMENTS_PER_BULK_REQUEST = 300;
    private static final int CHARACTERS_PER_DOCUMENT = 8_000;

    private S3ChecksumRestTestSupport() {}

    private static boolean isChecksumRequestHeader(String headerName) {
        final var lowerCaseName = headerName.toLowerCase(Locale.ROOT);
        return lowerCaseName.startsWith("x-amz-checksum-")
            || lowerCaseName.equals("x-amz-trailer")
            || lowerCaseName.equals("x-amz-sdk-checksum-algorithm");
    }

    /** Fails if any of the request headers declares a checksum or a checksum trailer. */
    static void assertNoChecksumRequestHeaders(Set<String> requestHeaderNames) {
        for (final var headerName : requestHeaderNames) {
            assertFalse("unexpected checksum header [" + headerName + "]", isChecksumRequestHeader(headerName));
        }
    }

    /** Fails unless at least one of the request headers declares a checksum or a checksum trailer. */
    static void assertSomeChecksumRequestHeader(Set<String> requestHeaderNames) {
        assertTrue(
            "expected a checksum header or trailer declaration in " + requestHeaderNames,
            requestHeaderNames.stream().anyMatch(S3ChecksumRestTestSupport::isChecksumRequestHeader)
        );
    }

    /** Indexes at least {@code minimumBytes} of random text, which doesn't compress, into the index. */
    static void indexIncompressibleData(RestClient client, String indexName, long minimumBytes) throws IOException {
        long indexedBytes = 0;
        while (indexedBytes < minimumBytes) {
            final var bulk = new StringBuilder();
            for (int doc = 0; doc < DOCUMENTS_PER_BULK_REQUEST; doc++) {
                bulk.append("{\"index\":{}}\n{\"payload\":{\"text\":\"")
                    .append(randomAlphaOfLength(CHARACTERS_PER_DOCUMENT))
                    .append("\"}}\n");
            }
            final var bulkRequest = new Request("POST", "/" + indexName + "/_bulk");
            bulkRequest.setJsonEntity(bulk.toString());
            assertFalse(assertOKAndCreateObjectPath(client.performRequest(bulkRequest)).evaluate("errors"));
            indexedBytes += (long) DOCUMENTS_PER_BULK_REQUEST * CHARACTERS_PER_DOCUMENT;
        }
    }

    /** Takes a snapshot of everything and waits for it, failing unless it finished with no failed shards. */
    static void createSnapshotAndAssertSuccess(RestClient client, String repositoryName, String snapshotName) throws IOException {
        final var request = new Request("PUT", "/_snapshot/" + repositoryName + "/" + snapshotName);
        request.addParameter("wait_for_completion", "true");
        final var response = assertOKAndCreateObjectPath(client.performRequest(request));
        assertEquals("SUCCESS", response.evaluate("snapshot.state"));
        assertThat(response.evaluate("snapshot.shards.failed"), equalTo(0));
    }

    static void deleteSnapshotAndAssertOk(RestClient client, String repositoryName, String snapshotName) throws IOException {
        assertOK(client.performRequest(new Request("DELETE", "/_snapshot/" + repositoryName + "/" + snapshotName)));
    }
}
