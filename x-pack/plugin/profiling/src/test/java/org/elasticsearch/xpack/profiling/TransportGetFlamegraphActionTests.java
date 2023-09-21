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

public class TransportGetFlamegraphActionTests extends ESTestCase {
    public void testCreateFlamegraph() {
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
            Map.of("2buqP1GpF-TXYmL4USW8gA", 1),
            9,
            1.0d
        );
        GetFlamegraphResponse response = TransportGetFlamegraphAction.buildFlamegraph(stacktraces);
        assertNotNull(response);
        assertEquals(10, response.getSize());
        assertEquals(1.0d, response.getSamplingRate(), 0.001d);
        assertEquals(List.of(1, 1, 1, 1, 1, 1, 1, 1, 1, 1), response.getCountInclusive());
        assertEquals(List.of(0, 0, 0, 0, 0, 0, 0, 0, 0, 1), response.getCountExclusive());

    }
}
