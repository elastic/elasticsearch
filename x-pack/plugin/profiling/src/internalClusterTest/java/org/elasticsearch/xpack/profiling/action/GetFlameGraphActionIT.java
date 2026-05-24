/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.action;

public class GetFlameGraphActionIT extends ProfilingTestCase {
    public void testGetStackTracesUnfiltered() throws Exception {
        GetStackTracesRequest request = new GetStackTracesRequest(
            1000,
            600.0d,
            1.0d,
            1.0d,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
        GetFlamegraphResponse response = client().execute(GetFlamegraphAction.INSTANCE, request).get();
        // only spot-check top level properties - detailed tests are done in unit tests
        assertEquals(1008, response.getSize());
        assertEquals(1.0d, response.getSamplingRate(), 0.001d);
        assertEquals(45, response.getSelfCPU());
        assertEquals(1974, response.getTotalCPU());
        assertEquals(45, response.getTotalSamples());

        // The root node's values are the same as the top-level values.
        assertEquals("", response.getFileIds().get(0));
        assertEquals(response.getSelfCPU(), response.getCountInclusive().get(0).longValue());
    }
}
