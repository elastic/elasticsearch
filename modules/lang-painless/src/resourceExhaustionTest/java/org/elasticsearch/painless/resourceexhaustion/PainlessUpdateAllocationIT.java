/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless.resourceexhaustion;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

/**
 * Stress-tests the Painless allocation limit in the {@code update} context against a
 * heap-constrained node. The cluster runs with a 512 MB heap and a 200 MB per-execution
 * allocation limit. Scripts loop-allocate 2 MB chunks: 50 iterations (100 MB) succeed
 * and write the total to the document; 150 iterations throw a {@code PainlessError} when
 * the running total crosses 200 MB, before the excess heap is ever touched.
 *
 * <p>The allocation check fires before each {@code new} instruction, so the failure path
 * never exceeds the limit in actual heap usage — it stops at the chunk that would push
 * the running total over the threshold.
 */
public class PainlessUpdateAllocationIT extends ResourceExhaustionPainlessTestCase {

    private static final String INDEX = "painless-update-alloc";
    private static final String DOC_ID = "1";
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
        .setting("script.painless.max_allocation_bytes.context.update.limit", "200mb")
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

        Request doc = new Request("PUT", "/" + INDEX + "/_doc/" + DOC_ID);
        doc.setJsonEntity("{\"value\": 0}");
        client().performRequest(doc);
    }

    public void testUpdateScriptUnderLimitSucceeds() throws IOException {
        assertThat(client().performRequest(updateScript(SUCCESS_ITERS)).getStatusLine().getStatusCode(), equalTo(200));
    }

    public void testUpdateScriptOverLimitFails() throws IOException {
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(updateScript(FAILURE_ITERS)));
        assertAllocationLimitExceeded(e);
    }

    private Request updateScript(int iters) {
        String script = "long total = 0; for (int i = 0; i < "
            + iters
            + "; i++) { byte[] chunk = new byte["
            + CHUNK_BYTES
            + "]; total += chunk.length; } ctx._source.value = total;";
        Request update = new Request("POST", "/" + INDEX + "/_update/" + DOC_ID);
        update.setJsonEntity("{\"script\":{\"source\":\"" + script + "\"}}");
        return update;
    }
}
