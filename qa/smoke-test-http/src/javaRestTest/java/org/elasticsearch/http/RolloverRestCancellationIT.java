/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.action.admin.indices.rollover.RolloverAction;
import org.elasticsearch.client.Request;

public class RolloverRestCancellationIT extends BlockedSearcherRestCancellationTestCase {

    public void testRolloverRestCancellation() throws Exception {
        runTest(new Request(HttpPost.METHOD_NAME, "test/_rollover"), RolloverAction.NAME);
    }
}
