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

public class TransportGetTopNFunctionsActionTests extends ESTestCase {
    public void testCreateAllTopNFunctions() {
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

        GetTopNFunctionsResponse response = TransportGetTopNFunctionsAction.buildTopNFunctions(stacktraces, null);
        assertNotNull(response);
        assertEquals(1, response.getSelfCount());
        assertEquals(9, response.getTotalCount());

        List<TopNFunction> topNFunctions = response.getTopN();
        assertNotNull(topNFunctions);
        assertEquals(9, topNFunctions.size());

        assertEquals(
            List.of(
                topN("178196121", 1, 16339645, 1L, 1L, 0.3d, 0.3d, 2.7d, 2.7d),
                topN("181192637", 2, 19336161, 0L, 1L, 0.0d, 0.3d, 0.0d, 2.7d),
                topN("181190529", 3, 19334053, 0L, 1L, 0.0d, 0.3d, 0.0d, 2.7d),
                topN("180652335", 4, 18795859, 0L, 1L, 0.0d, 0.3d, 0.0d, 2.7d),
                topN("180479184", 5, 18622708, 0L, 1L, 0.0d, 0.3d, 0.0d, 2.7d),
                topN("180475689", 6, 18619213, 0L, 1L, 0.0d, 0.3d, 0.0d, 2.7d),
                topN("175515318", 7, 13658842, 0L, 1L, 0.0d, 0.3d, 0.0d, 2.7d),
                topN("174846197", 8, 12989721, 0L, 1L, 0.0d, 0.3d, 0.0d, 2.7d),
                topN("174640828", 9, 12784352, 0L, 1L, 0.0d, 0.3d, 0.0d, 2.7d)
            ),
            topNFunctions
        );
    }

    public void testCreateTopNFunctionsWithLimit() {
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

        GetTopNFunctionsResponse response = TransportGetTopNFunctionsAction.buildTopNFunctions(stacktraces, 3);
        assertNotNull(response);
        assertEquals(1, response.getSelfCount());
        assertEquals(9, response.getTotalCount());

        List<TopNFunction> topNFunctions = response.getTopN();
        assertNotNull(topNFunctions);
        assertEquals(3, topNFunctions.size());

        assertEquals(
            List.of(
                topN("178196121", 1, 16339645, 1L, 1L, 0.3d, 0.3d, 2.7d, 2.7d),
                topN("181192637", 2, 19336161, 0L, 1L, 0.0d, 0.3d, 0.0d, 2.7d),
                topN("181190529", 3, 19334053, 0L, 1L, 0.0d, 0.3d, 0.0d, 2.7d)
            ),
            topNFunctions
        );
    }

    private TopNFunction topN(
        String id,
        int rank,
        int addressOrLine,
        long exclusiveCount,
        long inclusiveCount,
        double annualCO2TonsExclusive,
        double annualCO2TonsInclusive,
        double annualCostsUSDExclusive,
        double annualCostsUSDInclusive
    ) {
        return new TopNFunction(
            id,
            rank,
            3,
            false,
            addressOrLine,
            "",
            "",
            0,
            "containerd",
            exclusiveCount,
            inclusiveCount,
            annualCO2TonsExclusive,
            annualCO2TonsInclusive,
            annualCostsUSDExclusive,
            annualCostsUSDInclusive,
            null
        );
    }

    public void testCreateEmptyTopNFunctions() {
        GetStackTracesResponse stacktraces = new GetStackTracesResponse(Map.of(), Map.of(), Map.of(), Map.of(), 0, 1.0d, 0);
        GetTopNFunctionsResponse response = TransportGetTopNFunctionsAction.buildTopNFunctions(stacktraces, null);
        assertNotNull(response);
        assertEquals(0, response.getSelfCount());
        assertEquals(0, response.getTotalCount());

        List<TopNFunction> topNFunctions = response.getTopN();
        assertNotNull(topNFunctions);
        assertEquals(0, topNFunctions.size());
    }
}
