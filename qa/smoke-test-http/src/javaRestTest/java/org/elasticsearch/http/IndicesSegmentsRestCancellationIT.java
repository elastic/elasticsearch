/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsAction;
import org.elasticsearch.client.Request;
import org.elasticsearch.test.junit.annotations.TestLogging;

@TestLogging(value = "org.elasticsearch.tasks.TaskManager:TRACE,org.elasticsearch.test.TaskAssertions:TRACE", reason = "debugging")
public class IndicesSegmentsRestCancellationIT extends BlockedSearcherRestCancellationTestCase {
    public void testIndicesSegmentsRestCancellation() throws Exception {
        runTest(new Request(HttpGet.METHOD_NAME, "/_segments"), IndicesSegmentsAction.NAME);
    }

    public void testCatSegmentsRestCancellation() throws Exception {
        runTest(new Request(HttpGet.METHOD_NAME, "/_cat/segments"), IndicesSegmentsAction.NAME);
    }

}
