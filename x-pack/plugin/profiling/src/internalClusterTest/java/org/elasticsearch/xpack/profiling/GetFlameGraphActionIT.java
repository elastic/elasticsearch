/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling;

public class GetFlameGraphActionIT extends ProfilingTestCase {
    public void testGetStackTracesUnfiltered() throws Exception {
        GetStackTracesRequest request = new GetStackTracesRequest(1000, 600.0d, 1.0d, null, null, null, null, null, null, null, null);
        GetFlamegraphResponse response = client().execute(GetFlamegraphAction.INSTANCE, request).get();
        // only spot-check top level properties - detailed tests are done in unit tests
        assertEquals(994, response.getSize());
        assertEquals(1.0d, response.getSamplingRate(), 0.001d);
        assertEquals(44, response.getSelfCPU());
        assertEquals(1865, response.getTotalCPU());
        assertEquals(1.3651d, response.getSelfAnnualCostsUSD(), 0.0001d);
        assertEquals(0.000144890d, response.getSelfAnnualCO2Tons(), 0.000000001d);
        assertEquals(44, response.getTotalSamples());
    }
}
