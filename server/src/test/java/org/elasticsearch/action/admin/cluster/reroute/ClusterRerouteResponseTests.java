/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.reroute;

import org.elasticsearch.cluster.routing.allocation.RerouteExplanation;
import org.elasticsearch.cluster.routing.allocation.RoutingExplanations;
import org.elasticsearch.cluster.routing.allocation.command.AllocateReplicaAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

public class ClusterRerouteResponseTests extends ESTestCase {

    public void testToXContentWithExplain() {
        assertXContent(createClusterRerouteResponse(), new ToXContent.MapParams(Map.of("explain", "true", "metric", "none")), 2, """
            {
              "acknowledged": true,
              "explanations": [
                {
                  "command": "allocate_replica",
                  "parameters": {
                    "index": "index",
                    "shard": 0,
                    "node": "node0"
                  },
                  "decisions": [
                    {
                      "decider": null,
                      "decision": "YES",
                      "explanation": "none"
                    }
                  ]
                }
              ]
            }""");
    }

    private void assertXContent(
        ClusterRerouteResponse response,
        ToXContent.Params params,
        int expectedChunks,
        String expectedBody,
        String... criticalDeprecationWarnings
    ) {
        try {
            var builder = jsonBuilder();
            if (randomBoolean()) {
                builder.prettyPrint();
            }
            ChunkedToXContent.wrapAsToXContent(response).toXContent(builder, params);
            assertEquals(XContentHelper.stripWhitespace(expectedBody), XContentHelper.stripWhitespace(Strings.toString(builder)));
        } catch (IOException e) {
            throw new AssertionError("unexpected", e);
        }

        AbstractChunkedSerializingTestCase.assertChunkCount(response, params, ignored -> expectedChunks);
        assertCriticalWarnings(criticalDeprecationWarnings);

        // check the v7 API too
        AbstractChunkedSerializingTestCase.assertChunkCount(new ChunkedToXContent() {
            @Override
            public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params outerParams) {
                return response.toXContentChunkedV7(outerParams);
            }

            @Override
            public boolean isFragment() {
                return response.isFragment();
            }
        }, params, ignored -> expectedChunks);
        // the v7 API should not emit any deprecation warnings
        assertCriticalWarnings();
    }

    private static ClusterRerouteResponse createClusterRerouteResponse() {
        return new ClusterRerouteResponse(
            true,
            new RoutingExplanations().add(new RerouteExplanation(new AllocateReplicaAllocationCommand("index", 0, "node0"), Decision.YES))
        );
    }
}
