/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsAction;
import org.elasticsearch.client.Request;

public class NodeStatsRestCancellationIT extends BlockedSearcherRestCancellationTestCase {
    public void testNodeStatsRestCancellation() throws Exception {
        runTest(new Request(HttpGet.METHOD_NAME, "/_nodes/stats"), NodesStatsAction.NAME);
    }
}
