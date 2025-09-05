/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.rest;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;

public class NodeRestUsageIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local().build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @SuppressWarnings("unchecked")
    public void testWithRestUsage() throws IOException {
        // First get the current usage figures
        String path = randomFrom("_nodes/usage", "_nodes/usage/rest_actions", "_nodes/usage/_all");
        Response beforeResponse = client().performRequest(new Request("GET", path));
        Map<String, Object> beforeResponseBodyMap = entityAsMap(beforeResponse);
        assertThat(beforeResponseBodyMap, notNullValue());
        int beforeSuccessful = assertSuccess(beforeResponseBodyMap);

        Map<String, Object> beforeNodesMap = (Map<String, Object>) beforeResponseBodyMap.get("nodes");
        assertThat(beforeNodesMap, notNullValue());
        assertThat(beforeNodesMap.size(), equalTo(beforeSuccessful));
        Map<String, Long> beforeCombinedRestUsage = new HashMap<>();
        beforeCombinedRestUsage.put("nodes_usage_action", 0L);
        beforeCombinedRestUsage.put("create_index_action", 0L);
        beforeCombinedRestUsage.put("document_index_action", 0L);
        beforeCombinedRestUsage.put("search_action", 0L);
        beforeCombinedRestUsage.put("refresh_action", 0L);
        beforeCombinedRestUsage.put("cat_indices_action", 0L);
        beforeCombinedRestUsage.put("nodes_info_action", 0L);
        beforeCombinedRestUsage.put("nodes_stats_action", 0L);
        beforeCombinedRestUsage.put("delete_index_action", 0L);
        for (Map.Entry<String, Object> nodeEntry : beforeNodesMap.entrySet()) {
            Map<String, Object> beforeRestActionUsage = (Map<String, Object>) ((Map<String, Object>) nodeEntry.getValue()).get(
                "rest_actions"
            );
            assertThat(beforeRestActionUsage, notNullValue());
            for (Map.Entry<String, Object> restActionEntry : beforeRestActionUsage.entrySet()) {
                Long currentUsage = beforeCombinedRestUsage.get(restActionEntry.getKey());
                if (currentUsage == null) {
                    beforeCombinedRestUsage.put(restActionEntry.getKey(), ((Number) restActionEntry.getValue()).longValue());
                } else {
                    beforeCombinedRestUsage.put(restActionEntry.getKey(), currentUsage + ((Number) restActionEntry.getValue()).longValue());
                }
            }
        }

        // Do some requests to get some rest usage stats
        client().performRequest(new Request("PUT", "/test"));
        for (int i = 0; i < 3; i++) {
            final Request index = new Request("POST", "/test/_doc/1");
            index.setJsonEntity("{\"foo\": \"bar\"}");
            client().performRequest(index);
        }
        client().performRequest(new Request("GET", "/test/_search"));
        final Request index4 = new Request("POST", "/test/_doc/4");
        index4.setJsonEntity("{\"foo\": \"bar\"}");
        client().performRequest(index4);
        client().performRequest(new Request("POST", "/test/_refresh"));
        client().performRequest(new Request("GET", "/_cat/indices"));
        client().performRequest(new Request("GET", "/_nodes"));
        client().performRequest(new Request("GET", "/test/_search"));
        client().performRequest(new Request("GET", "/_nodes/stats"));
        client().performRequest(new Request("DELETE", "/test"));

        Response response = client().performRequest(new Request("GET", "_nodes/usage"));
        Map<String, Object> responseBodyMap = entityAsMap(response);
        assertThat(responseBodyMap, notNullValue());
        int successful = assertSuccess(responseBodyMap);

        Map<String, Object> nodesMap = (Map<String, Object>) responseBodyMap.get("nodes");
        assertThat(nodesMap, notNullValue());
        assertThat(nodesMap.size(), equalTo(successful));
        Map<String, Long> combinedRestUsage = new HashMap<>();
        for (Map.Entry<String, Object> nodeEntry : nodesMap.entrySet()) {
            Map<String, Object> restActionUsage = (Map<String, Object>) ((Map<String, Object>) nodeEntry.getValue()).get("rest_actions");
            assertThat(restActionUsage, notNullValue());
            for (Map.Entry<String, Object> restActionEntry : restActionUsage.entrySet()) {
                Long currentUsage = combinedRestUsage.get(restActionEntry.getKey());
                if (currentUsage == null) {
                    combinedRestUsage.put(restActionEntry.getKey(), ((Number) restActionEntry.getValue()).longValue());
                } else {
                    combinedRestUsage.put(restActionEntry.getKey(), currentUsage + ((Number) restActionEntry.getValue()).longValue());
                }
            }
        }
        assertThat(combinedRestUsage.get("nodes_usage_action") - beforeCombinedRestUsage.get("nodes_usage_action"), equalTo(1L));
        assertThat(combinedRestUsage.get("create_index_action") - beforeCombinedRestUsage.get("create_index_action"), equalTo(1L));
        assertThat(combinedRestUsage.get("document_index_action") - beforeCombinedRestUsage.get("document_index_action"), equalTo(4L));
        assertThat(combinedRestUsage.get("search_action") - beforeCombinedRestUsage.get("search_action"), equalTo(2L));
        assertThat(combinedRestUsage.get("refresh_action") - beforeCombinedRestUsage.get("refresh_action"), equalTo(1L));
        assertThat(combinedRestUsage.get("cat_indices_action") - beforeCombinedRestUsage.get("cat_indices_action"), equalTo(1L));
        assertThat(combinedRestUsage.get("nodes_info_action") - beforeCombinedRestUsage.get("nodes_info_action"), equalTo(1L));
        assertThat(combinedRestUsage.get("nodes_stats_action") - beforeCombinedRestUsage.get("nodes_stats_action"), equalTo(1L));
        assertThat(combinedRestUsage.get("delete_index_action") - beforeCombinedRestUsage.get("delete_index_action"), equalTo(1L));

    }

    public void testMetricsWithAll() throws IOException {
        ResponseException exception = expectThrows(
            ResponseException.class,
            () -> client().performRequest(new Request("GET", "_nodes/usage/_all,rest_actions"))
        );
        assertNotNull(exception);
        assertThat(exception.getMessage(), containsString("""
            "type":"illegal_argument_exception",\
            "reason":"request [_nodes/usage/_all,rest_actions] contains _all and individual metrics [_all,rest_actions]\""""));
    }

    @SuppressWarnings("unchecked")
    public void testAggregationUsage() throws IOException {
        // First get the current usage figures
        String path = randomFrom("_nodes/usage", "_nodes/usage/aggregations", "_nodes/usage/_all");
        Response beforeResponse = client().performRequest(new Request("GET", path));
        Map<String, Object> beforeResponseBodyMap = entityAsMap(beforeResponse);
        assertThat(beforeResponseBodyMap, notNullValue());
        int beforeSuccessful = assertSuccess(beforeResponseBodyMap);

        Map<String, Object> beforeNodesMap = (Map<String, Object>) beforeResponseBodyMap.get("nodes");
        assertThat(beforeNodesMap, notNullValue());
        assertThat(beforeNodesMap.size(), equalTo(beforeSuccessful));

        Map<String, Map<String, Long>> beforeCombinedAggsUsage = getTotalUsage(beforeNodesMap);
        // Do some requests to get some rest usage stats
        Request create = new Request("PUT", "/test");
        create.setJsonEntity("""
            {
              "mappings": {
                "properties": {
                  "str": {
                    "type": "keyword"
                  },
                  "foo": {
                    "type": "keyword"
                  },
                  "num": {
                    "type": "long"
                  },
                  "start": {
                    "type": "date"
                  }
                }
              }
            }""");
        client().performRequest(create);

        Request searchRequest = new Request("GET", "/test/_search");
        SearchSourceBuilder searchSource = new SearchSourceBuilder().aggregation(
            AggregationBuilders.terms("str_terms").field("str.keyword")
        ).aggregation(AggregationBuilders.terms("num_terms").field("num")).aggregation(AggregationBuilders.avg("num_avg").field("num"));
        searchRequest.setJsonEntity(Strings.toString(searchSource));
        searchRequest.setJsonEntity(Strings.toString(searchSource));
        client().performRequest(searchRequest);

        searchRequest = new Request("GET", "/test/_search");
        searchSource = new SearchSourceBuilder().aggregation(AggregationBuilders.terms("start").field("start"))
            .aggregation(AggregationBuilders.avg("num1").field("num"))
            .aggregation(AggregationBuilders.avg("num2").field("num"))
            .aggregation(AggregationBuilders.terms("foo").field("foo.keyword"));
        String r = Strings.toString(searchSource);
        searchRequest.setJsonEntity(Strings.toString(searchSource));
        client().performRequest(searchRequest);

        Response response = client().performRequest(new Request("GET", "_nodes/usage"));
        Map<String, Object> responseBodyMap = entityAsMap(response);
        assertThat(responseBodyMap, notNullValue());
        int successful = assertSuccess(responseBodyMap);

        Map<String, Object> nodesMap = (Map<String, Object>) responseBodyMap.get("nodes");
        assertThat(nodesMap, notNullValue());
        assertThat(nodesMap.size(), equalTo(successful));

        Map<String, Map<String, Long>> afterCombinedAggsUsage = getTotalUsage(nodesMap);

        assertDiff(beforeCombinedAggsUsage, afterCombinedAggsUsage, "terms", "numeric", 1L);
        assertDiff(beforeCombinedAggsUsage, afterCombinedAggsUsage, "terms", "date", 1L);
        assertDiff(beforeCombinedAggsUsage, afterCombinedAggsUsage, "terms", "keyword", 2L);
        assertDiff(beforeCombinedAggsUsage, afterCombinedAggsUsage, "avg", "numeric", 3L);
    }

    private void assertDiff(
        Map<String, Map<String, Long>> before,
        Map<String, Map<String, Long>> after,
        String agg,
        String vst,
        long diff
    ) {
        Long valBefore = before.getOrDefault(agg, Collections.emptyMap()).getOrDefault(vst, 0L);
        Long valAfter = after.getOrDefault(agg, Collections.emptyMap()).getOrDefault(vst, 0L);
        assertThat(agg + "." + vst, valAfter - valBefore, equalTo(diff));
    }

    private Map<String, Map<String, Long>> getTotalUsage(Map<String, Object> nodeUsage) {
        Map<String, Map<String, Long>> combined = new HashMap<>();
        for (Map.Entry<String, Object> nodeEntry : nodeUsage.entrySet()) {
            @SuppressWarnings("unchecked")
            Map<String, Object> beforeAggsUsage = (Map<String, Object>) ((Map<String, Object>) nodeEntry.getValue()).get("aggregations");
            assertThat(beforeAggsUsage, notNullValue());
            for (Map.Entry<String, Object> aggEntry : beforeAggsUsage.entrySet()) {
                @SuppressWarnings("unchecked")
                Map<String, Object> aggMap = (Map<String, Object>) aggEntry.getValue();
                Map<String, Long> combinedAggMap = combined.computeIfAbsent(aggEntry.getKey(), k -> new HashMap<>());
                for (Map.Entry<String, Object> valSourceEntry : aggMap.entrySet()) {
                    combinedAggMap.put(
                        valSourceEntry.getKey(),
                        combinedAggMap.getOrDefault(valSourceEntry.getKey(), 0L) + ((Number) valSourceEntry.getValue()).longValue()
                    );
                }
            }
        }
        return combined;
    }

    private int assertSuccess(Map<String, Object> responseBodyMap) {
        @SuppressWarnings("unchecked")
        Map<String, Object> nodesResultMap = (Map<String, Object>) responseBodyMap.get("_nodes");
        assertThat(nodesResultMap, notNullValue());
        Integer total = (Integer) nodesResultMap.get("total");
        Integer successful = (Integer) nodesResultMap.get("successful");
        Integer failed = (Integer) nodesResultMap.get("failed");
        assertThat(total, greaterThan(0));
        assertThat(successful, equalTo(total));
        assertThat(failed, equalTo(0));
        return successful;
    }

}
