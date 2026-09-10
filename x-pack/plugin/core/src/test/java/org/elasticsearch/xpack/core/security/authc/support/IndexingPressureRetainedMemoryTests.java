/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc.support;

import org.apache.lucene.tests.util.RamUsageTester;
import org.elasticsearch.action.bulk.BulkItemRequest;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.hamcrest.Matchers.lessThan;

/**
 * Indexing pressure admits work against a budget of <em>document payload bytes</em>, but each
 * in-flight request also retains memory that is not payload — most of it the {@link Authentication}
 * decoded from its header and held in the captured thread context. That cost is fixed per request,
 * so the same payload budget admits far more of it when documents are small.
 *
 * <p>The invariant here: <b>two workloads consuming the same accounted budget should retain
 * comparable memory.</b> Before {@code decode} shared instances, shrinking the documents inflated
 * the request count and with it the unaccounted per-request cost — 17.9x more retained heap for an
 * identical budget — without indexing pressure noticing.
 *
 * <p>Measured on the serverless index node that ran out of memory: 148MB accounted against a 154MB
 * primary limit, while the in-flight requests actually held 2.35x that. Sharing the authentication
 * brings it to 1.22x, so the limit again means what it says.
 */
public class IndexingPressureRetainedMemoryTests extends ESTestCase {

    private static final ShardId SHARD_ID = new ShardId(new Index("index", "uuid"), 0);

    /**
     * One client's credential, shaped like a Fleet-server API key: role descriptors serialised
     * inline in metadata. Encoded once, because a single client sends the same header on every
     * request.
     */
    private static String oneClientsAuthenticationHeader() throws IOException {
        final BytesArray descriptors = new BytesArray("{\"role\":{\"indices\":[\"" + "x".repeat(4096) + "\"]}}");
        return AuthenticationTestHelper.builder()
            .apiKey("fleet-server-key")
            .metadata(
                Map.of(
                    AuthenticationField.API_KEY_ROLE_DESCRIPTORS_KEY,
                    descriptors,
                    AuthenticationField.API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY,
                    descriptors
                )
            )
            .build(false)
            .encode();
    }

    /**
     * One in-flight request: the bulk shard request, plus the transient-header map carrying its
     * {@link Authentication} for as long as the request is in flight. The {@code ThreadContext}
     * itself is per-thread and shared, but each captured context has its own map.
     */
    private record InFlight(BulkShardRequest request, Map<String, Object> transientHeaders) {}

    /** Fills {@code payloadBudget} accounted bytes with documents of {@code docSize}. */
    private static InFlight[] admitUpToBudget(String authHeader, long payloadBudget, int docSize) throws IOException {
        final String doc = "{\"f\":\"" + "y".repeat(docSize) + "\"}";
        long accounted = 0;
        int count = 0;
        final InFlight[] scratch = new InFlight[100_000];
        while (accounted < payloadBudget && count < scratch.length) {
            // A distinct payload buffer per request: on the wire each request owns its own.
            final IndexRequest indexRequest = new IndexRequest("index").id("id" + count)
                .source(new BytesArray(doc.getBytes(StandardCharsets.UTF_8)), XContentType.JSON);
            final BulkShardRequest request = new BulkShardRequest(
                SHARD_ID,
                SplitShardCountSummary.IRRELEVANT,
                RefreshPolicy.NONE,
                new BulkItemRequest[] { new BulkItemRequest(0, indexRequest) }
            );
            // Each request decodes the header for itself, exactly as the transport layer does.
            final Authentication authentication = AuthenticationContextSerializer.decode(authHeader);
            scratch[count++] = new InFlight(request, Map.of(AuthenticationField.AUTHENTICATION_KEY, authentication));
            accounted += request.ramBytesUsed(); // what IndexingPressure reserves
        }
        final InFlight[] inFlight = new InFlight[count];
        System.arraycopy(scratch, 0, inFlight, 0, count);
        return inFlight;
    }

    public void testSameAccountedBudgetRetainsComparableMemory() throws Exception {
        final String authHeader = oneClientsAuthenticationHeader();
        final long payloadBudget = 4_000_000L;

        final InFlight[] largeDocs = admitUpToBudget(authHeader, payloadBudget, 32 * 1024);
        final InFlight[] smallDocs = admitUpToBudget(authHeader, payloadBudget, 128);

        final long retainedByLargeDocs = RamUsageTester.ramUsed(largeDocs);
        final long retainedBySmallDocs = RamUsageTester.ramUsed(smallDocs);

        logger.info(
            "same {} byte budget -> {} large-doc requests retain {} bytes; {} small-doc requests retain {} bytes (ratio {})",
            payloadBudget,
            largeDocs.length,
            retainedByLargeDocs,
            smallDocs.length,
            retainedBySmallDocs,
            (double) retainedBySmallDocs / retainedByLargeDocs
        );

        // Equal accounted budget must mean comparable real memory, whatever the document size.
        // Generous factor: this guards against unbounded growth, not a tight bound. Any future
        // per-request object that indexing pressure does not account for pushes it back up.
        assertThat(retainedBySmallDocs, lessThan(retainedByLargeDocs * 2));
    }
}
