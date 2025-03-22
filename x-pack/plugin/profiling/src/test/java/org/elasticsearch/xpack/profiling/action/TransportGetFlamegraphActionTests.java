/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.action;

import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;

public class TransportGetFlamegraphActionTests extends ESTestCase {
    public void testCreateFlamegraph() {
        TraceEvent traceEvent = new TraceEvent(1L);
        traceEvent.annualCO2Tons = 0.3d;
        traceEvent.annualCostsUSD = 2.7d;

        GetStackTracesResponse stacktraces = new GetStackTracesResponse(
            Map.of(
                "2buqP1GpF-TXYmL4USW8gA",
                new StackTrace(
                    new int[] { 12784352, 19334053, 19336161, 18795859, 18622708, 18619213, 12989721, 13658842, 16339645 },
                    new String[] {
                        "fr28zxcZ2UDasxYuu6dV-w",
                        "fr28zxcZ2UDasxYuu6dV-w",
                        "fr28zxcZ2UDasxYuu6dV-w",
                        "fr28zxcZ2UDasxYuu6dV-w",
                        "fr28zxcZ2UDasxYuu6dV-w",
                        "fr28zxcZ2UDasxYuu6dV-w",
                        "fr28zxcZ2UDasxYuu6dV-w",
                        "fr28zxcZ2UDasxYuu6dV-w",
                        "fr28zxcZ2UDasxYuu6dV-w" },
                    new String[] {
                        "fr28zxcZ2UDasxYuu6dV-wAAAAAAwxLg",
                        "fr28zxcZ2UDasxYuu6dV-wAAAAABJwOl",
                        "fr28zxcZ2UDasxYuu6dV-wAAAAABJwvh",
                        "fr28zxcZ2UDasxYuu6dV-wAAAAABHs1T",
                        "fr28zxcZ2UDasxYuu6dV-wAAAAABHCj0",
                        "fr28zxcZ2UDasxYuu6dV-wAAAAABHBtN",
                        "fr28zxcZ2UDasxYuu6dV-wAAAAAAxjUZ",
                        "fr28zxcZ2UDasxYuu6dV-wAAAAAA0Gra",
                        "fr28zxcZ2UDasxYuu6dV-wAAAAAA-VK9" },
                    new int[] { 3, 3, 3, 3, 3, 3, 3, 3, 3 }
                )
            ),
            Map.of(),
            Map.of("fr28zxcZ2UDasxYuu6dV-w", "containerd"),
            Map.of(new TraceEventID("", "", "", "2buqP1GpF-TXYmL4USW8gA"), traceEvent),
            9,
            1.0d,
            1
        );
        GetFlamegraphResponse response = TransportGetFlamegraphAction.buildFlamegraph(stacktraces);
        assertNotNull(response);
        assertEquals(12, response.getSize());
        assertEquals(1.0d, response.getSamplingRate(), 0.001d);
        assertEquals(List.of(1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L), response.getCountInclusive());
        assertEquals(List.of(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 1L), response.getCountExclusive());
        assertEquals(
            List.of(
                Map.of("1220", 1),
                Map.of("1219", 2),
                Map.of("174640828", 3),
                Map.of("181190529", 4),
                Map.of("181192637", 5),
                Map.of("180652335", 6),
                Map.of("180479184", 7),
                Map.of("180475689", 8),
                Map.of("174846197", 9),
                Map.of("175515318", 10),
                Map.of("178196121", 11),
                Map.of()
            ),
            response.getEdges()
        );
        assertEquals(
            List.of(
                "",
                "",
                "",
                "fr28zxcZ2UDasxYuu6dV-w",
                "fr28zxcZ2UDasxYuu6dV-w",
                "fr28zxcZ2UDasxYuu6dV-w",
                "fr28zxcZ2UDasxYuu6dV-w",
                "fr28zxcZ2UDasxYuu6dV-w",
                "fr28zxcZ2UDasxYuu6dV-w",
                "fr28zxcZ2UDasxYuu6dV-w",
                "fr28zxcZ2UDasxYuu6dV-w",
                "fr28zxcZ2UDasxYuu6dV-w"
            ),
            response.getFileIds()
        );
        assertEquals(
            List.of(
                TransportGetFlamegraphAction.FRAMETYPE_ROOT,
                TransportGetFlamegraphAction.FRAMETYPE_EXECUTABLE,
                TransportGetFlamegraphAction.FRAMETYPE_THREAD,
                3,
                3,
                3,
                3,
                3,
                3,
                3,
                3,
                3
            ),
            response.getFrameTypes()
        );
        assertEquals(
            List.of(false, false, false, false, false, false, false, false, false, false, false, false),
            response.getInlineFrames()
        );
        assertEquals(
            List.of(
                "",
                "",
                "",
                "containerd",
                "containerd",
                "containerd",
                "containerd",
                "containerd",
                "containerd",
                "containerd",
                "containerd",
                "containerd"
            ),
            response.getFileNames()
        );
        assertEquals(
            List.of(0, 0, 0, 12784352, 19334053, 19336161, 18795859, 18622708, 18619213, 12989721, 13658842, 16339645),
            response.getAddressOrLines()
        );
        assertEquals(List.of("", "", "", "", "", "", "", "", "", "", "", ""), response.getFunctionNames());
        assertEquals(List.of(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), response.getFunctionOffsets());
        assertEquals(List.of("", "", "", "", "", "", "", "", "", "", "", ""), response.getSourceFileNames());
        assertEquals(List.of(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), response.getSourceLines());
        assertEquals(1L, response.getSelfCPU());
        assertEquals(12L, response.getTotalCPU());
        assertEquals(1L, response.getTotalSamples());
    }

    public void testCreateEmptyFlamegraphWithRootNode() {
        GetStackTracesResponse stacktraces = new GetStackTracesResponse(Map.of(), Map.of(), Map.of(), Map.of(), 0, 1.0d, 0);
        GetFlamegraphResponse response = TransportGetFlamegraphAction.buildFlamegraph(stacktraces);
        assertNotNull(response);
        assertEquals(1, response.getSize());
        assertEquals(1.0d, response.getSamplingRate(), 0.001d);
        assertEquals(List.of(0L), response.getCountInclusive());
        assertEquals(List.of(0L), response.getCountExclusive());
        assertEquals(List.of(Map.of()), response.getEdges());
        assertEquals(List.of(""), response.getFileIds());
        assertEquals(List.of(TransportGetFlamegraphAction.FRAMETYPE_ROOT), response.getFrameTypes());
        assertEquals(List.of(false), response.getInlineFrames());
        assertEquals(List.of(""), response.getFileNames());
        assertEquals(List.of(0), response.getAddressOrLines());
        assertEquals(List.of(""), response.getFunctionNames());
        assertEquals(List.of(0), response.getFunctionOffsets());
        assertEquals(List.of(""), response.getSourceFileNames());
        assertEquals(List.of(0), response.getSourceLines());
        assertEquals(0L, response.getSelfCPU());
        assertEquals(0L, response.getTotalCPU());
        assertEquals(0L, response.getTotalSamples());
    }
}
