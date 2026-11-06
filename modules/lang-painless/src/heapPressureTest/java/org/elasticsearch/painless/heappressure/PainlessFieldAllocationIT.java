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
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

/**
 * A {@code script_fields} script in the {@code field} context that allocates more than the heap must fail the request, not kill
 * the node.
 *
 * <p>The cluster runs with the node defaults, as a production node does: no
 * {@code script.painless.max_allocation_bytes.context.field.limit}, so allocation tracking is off. The heap is 256 MB, and each
 * script asks for about 1 GB during the fetch phase, either by doubling a string or by holding 1 MB arrays in a list. Today the
 * allocation throws {@link OutOfMemoryError} on a search thread and the node exits. Once the {@code field} context has a default
 * limit, the script fails with a {@code painless_error} and the node stays up.
 */
public class PainlessFieldAllocationIT extends HeapPressurePainlessTestCase {

    private static final String INDEX = "painless-field-alloc";
    // 2^30 one-byte chars: the last doubling needs a 1 GB string.
    private static final int STRING_DOUBLINGS = 30;
    // 512,000 chars is 1 MB per array; 1,000 of them kept alive is 1 GB.
    private static final int CHUNK_CHARS = 512_000;
    private static final int CHUNKS = 1_000;

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .nodes(1)
        .module("lang-painless")
        .setting("xpack.security.enabled", "false")
        .jvmArg("-Xms256m")
        .jvmArg("-Xmx256m")
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
        doc.addParameter("refresh", "true");
        doc.setJsonEntity("{\"value\": 1}");
        client().performRequest(doc);
    }

    public void testStringDoublingFailsRequestNotNode() throws IOException {
        // Concatenated so integer literals are always ASCII, regardless of the randomized test locale.
        String script = "String s = 'A'; for (int i = 0; i < " + STRING_DOUBLINGS + "; i++) { s += s; } return s.length();";
        assertScriptFailsAndNodeSurvives(script);
    }

    public void testRetainedArraysFailRequestNotNode() throws IOException {
        String script = "List hog = new ArrayList(); for (int i = 0; i < "
            + CHUNKS
            + "; i++) { char[] chunk = new char["
            + CHUNK_CHARS
            + "]; hog.add(chunk); } return hog.size();";
        assertScriptFailsAndNodeSurvives(script);
    }

    private void assertScriptFailsAndNodeSurvives(String script) throws IOException {
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(scriptFieldsSearch(script)));
        assertAllocationLimitExceeded(e);
        // A node that ran out of memory has exited and would not answer.
        assertThat(client().performRequest(new Request("GET", "/_cluster/health")).getStatusLine().getStatusCode(), equalTo(200));
    }

    private Request scriptFieldsSearch(String script) {
        Request search = new Request("POST", "/" + INDEX + "/_search");
        search.setJsonEntity("{\"query\":{\"match_all\":{}},\"script_fields\":{\"test\":{\"script\":{\"source\":\"" + script + "\"}}}}");
        return search;
    }
}
