/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling;

import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TransportGetTopNFunctionsActionTests extends ESTestCase {
    public void testCreateTopNFunctions() {
        GetStackTracesResponse stacktraces = new GetStackTracesResponse(
            Map.of(
                "2buqP1GpF-TXYmL4USW8gA",
                new StackTrace(
                    List.of(12784352, 19334053, 19336161, 18795859, 18622708, 18619213, 12989721, 13658842, 16339645),
                    List.of(
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
                    List.of(
                        "fr28zxcZ2UDasxYuu6dV-wAAAAAAwxLg",
                        "fr28zxcZ2UDasxYuu6dV-wAAAAABJwOl",
                        "fr28zxcZ2UDasxYuu6dV-wAAAAABJwvh",
                        "fr28zxcZ2UDasxYuu6dV-wAAAAABHs1T",
                        "fr28zxcZ2UDasxYuu6dV-wAAAAABHCj0",
                        "fr28zxcZ2UDasxYuu6dV-wAAAAABHBtN",
                        "fr28zxcZ2UDasxYuu6dV-wAAAAAAxjUZ",
                        "fr28zxcZ2UDasxYuu6dV-wAAAAAA0Gra",
                        "fr28zxcZ2UDasxYuu6dV-wAAAAAA-VK9"
                    ),
                    List.of(3, 3, 3, 3, 3, 3, 3, 3, 3)
                )
            ),
            Map.of(),
            Map.of("fr28zxcZ2UDasxYuu6dV-w", "containerd"),
            Map.of("2buqP1GpF-TXYmL4USW8gA", 1L),
            9,
            1.0d,
            1
        );

        GetTopNFunctionsResponse response = TransportGetTopNFunctionsAction.buildTopNFunctions(stacktraces);
        assertNotNull(response);
        assertEquals(1.0d, response.getSamplingRate(), 0.001d);
        assertEquals(1, response.getTotalCount());
        assertEquals(1, response.getSelfCPU());
        assertEquals(9, response.getTotalCPU());

        List<TopNFunction> topNFunctions = response.getTopN();
        assertNotNull(topNFunctions);
        assertEquals(9, topNFunctions.size());

        List<String> ids = topNFunctions.stream().map(fn -> fn.id).collect(Collectors.toList());
        List<Integer> ranks = topNFunctions.stream().map(fn -> fn.rank).collect(Collectors.toList());
        List<Long> exclusiveCounts = topNFunctions.stream().map(fn -> fn.exclusiveCount).collect(Collectors.toList());
        List<Long> inclusiveCounts = topNFunctions.stream().map(fn -> fn.inclusiveCount).collect(Collectors.toList());
        List<String> frameIDs = topNFunctions.stream().map(fn -> fn.metadata.frameID).collect(Collectors.toList());
        List<String> fileIDs = topNFunctions.stream().map(fn -> fn.metadata.fileID).collect(Collectors.toList());
        List<Integer> frameTypes = topNFunctions.stream().map(fn -> fn.metadata.frameType).collect(Collectors.toList());
        List<Boolean> inlines = topNFunctions.stream().map(fn -> fn.metadata.inline).collect(Collectors.toList());
        List<Integer> addressOrLines = topNFunctions.stream().map(fn -> fn.metadata.addressOrLine).collect(Collectors.toList());
        List<String> functionNames = topNFunctions.stream().map(fn -> fn.metadata.functionName).collect(Collectors.toList());
        List<Integer> functionOffsets = topNFunctions.stream().map(fn -> fn.metadata.functionOffset).collect(Collectors.toList());
        List<String> sourceFilenames = topNFunctions.stream().map(fn -> fn.metadata.sourceFilename).collect(Collectors.toList());
        List<Integer> sourceLines = topNFunctions.stream().map(fn -> fn.metadata.sourceLine).collect(Collectors.toList());
        List<String> exeFilenames = topNFunctions.stream().map(fn -> fn.metadata.exeFilename).collect(Collectors.toList());

        assertEquals(
            List.of("178196121", "181192637", "181190529", "180652335", "180479184", "180475689", "175515318", "174846197", "174640828"),
            ids
        );
        assertEquals(List.of(1, 2, 3, 4, 5, 6, 7, 8, 9), ranks);
        assertEquals(List.of(1L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L), exclusiveCounts);
        assertEquals(List.of(1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L), inclusiveCounts);
        assertEquals(
            List.of(
                "fr28zxcZ2UDasxYuu6dV-wAAAAAA-VK9",
                "fr28zxcZ2UDasxYuu6dV-wAAAAABJwvh",
                "fr28zxcZ2UDasxYuu6dV-wAAAAABJwOl",
                "fr28zxcZ2UDasxYuu6dV-wAAAAABHs1T",
                "fr28zxcZ2UDasxYuu6dV-wAAAAABHCj0",
                "fr28zxcZ2UDasxYuu6dV-wAAAAABHBtN",
                "fr28zxcZ2UDasxYuu6dV-wAAAAAA0Gra",
                "fr28zxcZ2UDasxYuu6dV-wAAAAAAxjUZ",
                "fr28zxcZ2UDasxYuu6dV-wAAAAAAwxLg"
            ),
            frameIDs
        );
        assertEquals(
            List.of(
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
            fileIDs
        );
        assertEquals(List.of(3, 3, 3, 3, 3, 3, 3, 3, 3), frameTypes);
        assertEquals(List.of(false, false, false, false, false, false, false, false, false), inlines);
        assertEquals(List.of(16339645, 19336161, 19334053, 18795859, 18622708, 18619213, 13658842, 12989721, 12784352), addressOrLines);
        assertEquals(List.of("", "", "", "", "", "", "", "", ""), functionNames);
        assertEquals(List.of(0, 0, 0, 0, 0, 0, 0, 0, 0), functionOffsets);
        assertEquals(List.of("", "", "", "", "", "", "", "", ""), sourceFilenames);
        assertEquals(List.of(0, 0, 0, 0, 0, 0, 0, 0, 0), sourceLines);
        assertEquals(
            List.of(
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
            exeFilenames
        );
    }

    public void testCreateEmptyTopNFunctions() {
        GetStackTracesResponse stacktraces = new GetStackTracesResponse(Map.of(), Map.of(), Map.of(), Map.of(), 0, 1.0d, 0);
        GetTopNFunctionsResponse response = TransportGetTopNFunctionsAction.buildTopNFunctions(stacktraces);
        assertNotNull(response);
        assertEquals(1.0d, response.getSamplingRate(), 0.001d);
        assertEquals(0, response.getTotalCount());
        assertEquals(0, response.getSelfCPU());
        assertEquals(0, response.getTotalCPU());

        List<TopNFunction> topNFunctions = response.getTopN();
        assertNotNull(topNFunctions);
        assertEquals(0, topNFunctions.size());
    }
}
