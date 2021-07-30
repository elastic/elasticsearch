/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.rollup;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

public class StartRollupJobRequestTests extends ESTestCase {

    public void testConstructor() {
        String jobId = randomAlphaOfLength(5);
        assertEquals(jobId, new StartRollupJobRequest(jobId).getJobId());
    }

    public void testEqualsAndHash() {
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(new StartRollupJobRequest(randomAlphaOfLength(5)),
                orig -> new StartRollupJobRequest(orig.getJobId()),
                orig -> new StartRollupJobRequest(orig.getJobId() + "_suffix"));
    }

    public void testRequireJobId() {
        final NullPointerException e = expectThrows(NullPointerException.class, ()-> new StartRollupJobRequest(null));
        assertEquals("id parameter must not be null", e.getMessage());
    }

}
