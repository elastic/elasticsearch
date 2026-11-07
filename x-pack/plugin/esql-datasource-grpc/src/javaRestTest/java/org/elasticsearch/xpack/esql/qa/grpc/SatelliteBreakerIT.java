/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.grpc;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.datasource.grpc.BlockingSatelliteServer;
import org.elasticsearch.xpack.esql.datasources.Federation;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * A satellite that answers with a result far bigger than the request breaker allows must trip the breaker (a 429), not exhaust the
 * heap, and the node must keep serving queries afterwards.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class SatelliteBreakerIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .nodes(1)
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting(Federation.FEDERATION_ENABLED.getKey(), "true")
        .setting("xpack.ml.enabled", "false")
        .setting("indices.breaker.request.limit", "10mb")
        .jvmArg("--add-opens=java.base/java.nio=ALL-UNNAMED")
        .jvmArg("-Darrow.allocation.manager.type=Unsafe")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testAnOversizedResultTripsTheBreakerAndTheNodeSurvives() throws Exception {
        try (BlockingSatelliteServer satellite = new BlockingSatelliteServer()) {
            Request source = new Request("PUT", "/_query/data_source/breaker_src");
            source.setJsonEntity("{\"type\": \"flight\"}");
            client().performRequest(source);
            Request dataset = new Request("PUT", "/_query/dataset/breaker_ds");
            dataset.setJsonEntity("{\"data_source\": \"breaker_src\", \"resource\": \"flight://127.0.0.1:" + satellite.port() + "/t\"}");
            client().performRequest(dataset);
            try {
                satellite.oneBatchOfRows = 8_000_000; // 32 MB of integers in one batch, against a 10 MB breaker
                Request query = new Request("POST", "/_query");
                query.setJsonEntity("{\"query\": \"FROM breaker_ds | KEEP id | LIMIT 5\"}");
                ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(query));
                assertEquals(e.getMessage(), 429, e.getResponse().getStatusLine().getStatusCode());
                assertThat(e.getMessage(), containsString("circuit_breaking_exception"));

                // the node is fine: a normal answer follows
                satellite.oneBatchOfRows = 0;
                Map<String, Object> ok = entityAsMap(client().performRequest(query));
                assertThat(ok.get("values"), equalTo(List.of(List.of(0), List.of(1), List.of(2))));
                assertBreakerBackToNormal();
            } finally {
                deleteQuietly("/_query/dataset/breaker_ds");
                deleteQuietly("/_query/data_source/breaker_src");
            }
        }
    }

    /** Nothing the failed query held is left charged to the request breaker. */
    @SuppressWarnings("unchecked")
    private void assertBreakerBackToNormal() throws Exception {
        assertBusy(() -> {
            Map<String, Object> stats = entityAsMap(client().performRequest(new Request("GET", "/_nodes/stats/breaker")));
            Map<String, Object> nodes = (Map<String, Object>) stats.get("nodes");
            for (Object node : nodes.values()) {
                Map<String, Object> breakers = (Map<String, Object>) ((Map<String, Object>) node).get("breakers");
                Map<String, Object> request = (Map<String, Object>) breakers.get("request");
                assertThat(
                    "the request breaker is empty again",
                    ((Number) request.get("estimated_size_in_bytes")).longValue(),
                    equalTo(0L)
                );
            }
        });
    }

    private void deleteQuietly(String path) throws IOException {
        try {
            client().performRequest(new Request("DELETE", path));
        } catch (ResponseException e) {
            // already gone
        }
    }
}
