/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.rollup;

import org.elasticsearch.test.ESTestCase;

public class GetRollupJobRequestTests extends ESTestCase {
    public void testRequiresJob() {
        final NullPointerException e = expectThrows(NullPointerException.class, () -> new GetRollupJobRequest(null));
        assertEquals("jobId is required", e.getMessage());
    }

    public void testDoNotUseAll() {
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new GetRollupJobRequest("_all"));
        assertEquals("use the default ctor to ask for all jobs", e.getMessage());
    }
}
