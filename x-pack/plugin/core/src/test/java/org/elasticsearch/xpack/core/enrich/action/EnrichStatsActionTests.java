/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.enrich.action;

import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction.Response;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class EnrichStatsActionTests extends ESTestCase {

    public void testFilterNodesFromResponse() throws IOException {
        int numberOfNodes = randomIntBetween(3, 20);
        List<Response.ExecutingPolicy> executingPolicies = new ArrayList<>();
        List<Response.CoordinatorStats> coordinatorStats = new ArrayList<>();
        List<Response.CacheStats> cacheStats = new ArrayList<>();
        for (int i = 0; i < numberOfNodes; i++) {
            String nodeId = randomAlphaOfLength(20);
            coordinatorStats.add(
                new Response.CoordinatorStats(
                    nodeId,
                    randomIntBetween(0, 1000),
                    randomIntBetween(0, 1000),
                    randomLongBetween(0, 100000),
                    randomLongBetween(0, 100000)
                )
            );
            cacheStats.add(
                new Response.CacheStats(
                    nodeId,
                    randomLongBetween(0, 100000),
                    randomLongBetween(0, 100000),
                    randomLongBetween(0, 10000),
                    randomIntBetween(0, 100000)
                )
            );
        }
        Response response = new Response(executingPolicies, coordinatorStats, cacheStats);
        // No params, we expect no filtering:
        assertToXContentResults(response, ToXContent.EMPTY_PARAMS, numberOfNodes);
        // responseRestricted=serverless, we expect coordinator stats and cache stats to be rolled up to a single object:
        assertToXContentResults(response, new ToXContent.MapParams(Map.of(RestRequest.RESPONSE_RESTRICTED, "serverless")), 1);
        // making sure we don't throw a NullPointerException
        assertToXContentResults(response, new ToXContent.MapParams(Collections.singletonMap("responseRestricted", null)), numberOfNodes);
        // wrong value for responseRestricted, we expect no filtering:
        assertToXContentResults(
            response,
            new ToXContent.MapParams(Map.of(RestRequest.RESPONSE_RESTRICTED, randomAlphaOfLength(5))),
            numberOfNodes
        );
    }

    @SuppressWarnings("unchecked")
    private void assertToXContentResults(Response response, ToXContent.Params toXContentParams, int expectedNumberOfNodes)
        throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, toXContentParams);
        Map<String, Object> responseMap = createParser(builder).map();
        List<Object> coordinatorStatsList = (List<Object>) responseMap.get("coordinator_stats");
        assertThat(coordinatorStatsList.size(), equalTo(expectedNumberOfNodes));
        List<Object> cacheStatsList = (List<Object>) responseMap.get("cache_stats");
        assertThat(cacheStatsList.size(), equalTo(expectedNumberOfNodes));
        if (expectedNumberOfNodes == 1) {
            assertThat(((Map<String, String>) coordinatorStatsList.get(0)).get("node_id"), equalTo("N/A"));
            assertThat(((Map<String, String>) cacheStatsList.get(0)).get("node_id"), equalTo("N/A"));
        }
    }
}
