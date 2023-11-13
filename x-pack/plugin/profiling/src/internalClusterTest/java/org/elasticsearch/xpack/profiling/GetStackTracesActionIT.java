/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling;

import java.util.List;

public class GetStackTracesActionIT extends ProfilingTestCase {
    public void testGetStackTracesUnfiltered() throws Exception {
        GetStackTracesRequest request = new GetStackTracesRequest(10, null);
        request.setAdjustSampleCount(true);
        GetStackTracesResponse response = client().execute(GetStackTracesAction.INSTANCE, request).get();
        assertEquals(40, response.getTotalSamples());
        assertEquals(285, response.getTotalFrames());

        assertNotNull(response.getStackTraceEvents());
        assertEquals(4, (int) response.getStackTraceEvents().get("14cFLjgoe-BTQd17mhedeA"));

        assertNotNull(response.getStackTraces());
        // just do a high-level spot check. Decoding is tested in unit-tests
        StackTrace stackTrace = response.getStackTraces().get("JvISdnJ47BQ01489cwF9DA");
        assertEquals(4, stackTrace.addressOrLines.size());
        assertEquals(4, stackTrace.fileIds.size());
        assertEquals(4, stackTrace.frameIds.size());
        assertEquals(4, stackTrace.typeIds.size());

        assertNotNull(response.getStackFrames());
        StackFrame stackFrame = response.getStackFrames().get("lHp5_WAgpLy2alrUVab6HAAAAAAATgeq");
        assertEquals(List.of("blkdev_issue_flush"), stackFrame.functionName);

        assertNotNull(response.getExecutables());
        assertNotNull("vmlinux", response.getExecutables().get("lHp5_WAgpLy2alrUVab6HA"));
    }
}
