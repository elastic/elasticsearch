/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless.heappressure;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

/**
 * Stress-tests the Painless allocation limit in the {@code score} context against a
 * heap-constrained node. The cluster runs with a 512 MB heap and a 200 MB per-execution
 * allocation limit. Scripts loop-allocate 2 MB chunks: 50 iterations (100 MB) succeed;
 * 150 iterations throw a {@code PainlessError} when the running total crosses 200 MB,
 * before the excess heap is ever touched.
 *
 * <p>The allocation check fires before each {@code new} instruction, so the failure path
 * never exceeds the limit in actual heap usage — it stops at the chunk that would push
 * the running total over the threshold.
 *
 * <p>The same cluster gives the {@code field} context a 16 KB limit, so a script field that
 * reads {@code params['_source']} shows the other kind of charge: the whole document, a floor
 * before the parse and the rest after it, paid once per execution no matter how little of it
 * the script reads.
 */
public class PainlessScoreAllocationIT extends HeapPressurePainlessTestCase {

    private static final String INDEX = "painless-score-alloc";
    // Each iteration allocates a 2 MB byte array.
    private static final int CHUNK_BYTES = 2 * 1024 * 1024;
    // 50 × 2 MB = 100 MB — safely under the 200 MB limit.
    private static final int SUCCESS_ITERS = 50;
    // 150 iterations would require 300 MB total; the limit fires at the 101st chunk (~202 MB).
    private static final int FAILURE_ITERS = 150;

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .nodes(1)
        .module("lang-painless")
        .setting("xpack.security.enabled", "false")
        .setting("script.painless.max_allocation_bytes.context.score.limit", "200mb")
        // A small limit for the field context, so a script field that reads _source can trip on a modest document.
        .setting("script.painless.max_allocation_bytes.context.field.limit", "16kb")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Before
    public void createTestIndex() throws IOException {
        Request create = new Request("PUT", "/" + INDEX);
        create.setJsonEntity("{\"settings\": {\"number_of_replicas\": 0}}");
        client().performRequest(create);

        Request doc = new Request("POST", "/" + INDEX + "/_doc");
        doc.setJsonEntity("{\"value\": 1}");
        client().performRequest(doc);

        client().performRequest(new Request("POST", "/" + INDEX + "/_refresh"));
    }

    public void testScoreScriptUnderLimitSucceeds() throws IOException {
        assertThat(client().performRequest(scoreSearch(SUCCESS_ITERS)).getStatusLine().getStatusCode(), equalTo(200));
    }

    public void testScoreScriptOverLimitFails() throws IOException {
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(scoreSearch(FAILURE_ITERS)));
        assertAllocationLimitExceeded(e);
    }

    public void testScriptFieldReadingSourceUnderLimitSucceeds() throws IOException {
        Response response = client().performRequest(sourceScriptFieldSearch());
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(ObjectPath.createFromResponse(response).evaluate("hits.hits.0.fields.value.0"), equalTo(1));
    }

    public void testScriptFieldReadingSourceOverLimitFails() throws IOException {
        // Reading _source loads and parses the whole document even though the script only reads the small value next to the
        // ten thousand character field. The loaded bytes and the parse floor alone pass the 16 KB limit, so the script stops
        // before the parse runs.
        Request doc = new Request("POST", "/" + INDEX + "/_doc");
        doc.setJsonEntity("{\"value\": 1, \"message\": \"" + "x".repeat(10_000) + "\"}");
        client().performRequest(doc);
        client().performRequest(new Request("POST", "/" + INDEX + "/_refresh"));
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(sourceScriptFieldSearch()));
        assertAllocationLimitExceeded(e);
    }

    /** A script field in the {@code field} context that reads {@code value} out of the parsed {@code _source}. */
    private Request sourceScriptFieldSearch() {
        Request search = new Request("POST", "/" + INDEX + "/_search");
        search.setJsonEntity(
            "{\"query\":{\"match_all\":{}},\"script_fields\":{\"value\":{\"script\":{\"source\":\"params['_source'].value\"}}}}"
        );
        return search;
    }

    private Request scoreSearch(int iters) {
        // Build the script source via concatenation so integer literals are always ASCII,
        // regardless of the randomized test locale.
        String script = "long total = 0; for (int i = 0; i < "
            + iters
            + "; i++) { byte[] chunk = new byte["
            + CHUNK_BYTES
            + "]; total += chunk.length; } return (double) total;";
        Request search = new Request("POST", "/" + INDEX + "/_search");
        search.setJsonEntity(
            "{\"query\":{\"function_score\":{\"query\":{\"match_all\":{}},"
                + "\"script_score\":{\"script\":{\"source\":\""
                + script
                + "\"}}}}}"
        );
        return search;
    }
}
