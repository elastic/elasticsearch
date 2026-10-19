/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.ccq;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/** Exercises typed metadata execution across clusters that advertise support. */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class FunctionalTimeSeriesCcsIT extends ESRestTestCase {
    private static final String INDEX = "functional-timeseries-ccs";
    private static final String QUERY_TIME = "2024-05-10T00:01:00Z";
    private static final List<String> REQUIRED_CAPABILITIES = List.of(
        EsqlCapabilities.Cap.PROMQL_COMMAND_V0.capabilityName(),
        EsqlCapabilities.Cap.PROMQL_FUNCTIONAL_TIMESERIES.capabilityName(),
        EsqlCapabilities.Cap.PROMQL_WITHOUT_GROUPING.capabilityName(),
        EsqlCapabilities.Cap.TS_COMMAND_V0.capabilityName()
    );

    private static final ElasticsearchCluster remoteCluster = Clusters.remoteCluster();
    private static final ElasticsearchCluster localCluster = Clusters.localCluster(remoteCluster, false);

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(remoteCluster).around(localCluster);

    @Override
    protected String getTestRestCluster() {
        return localCluster.getHttpAddresses();
    }

    /** A numeric label distinguishes the legacy source JSON from canonical string-valued metadata. */
    public void testMetadataRepresentationAcrossClusters() throws Exception {
        assumeTrue("local functional time-series metadata support is required", supportsMetadata(client()));
        try (RestClient remote = remoteClusterClient()) {
            assumeTrue("remote functional time-series metadata support is required", supportsMetadata(remote));
            createMetricsIndex(client());
            createMetricsIndex(remote);
            try {
                assertMetadata(INDEX, "7");
                assertMetadata(Clusters.REMOTE_CLUSTER_NAME + ":" + INDEX, "7");
            } finally {
                assertOK(remote.performRequest(new Request("DELETE", "/" + INDEX)));
            }
        }
    }

    private static boolean supportsMetadata(RestClient client) throws IOException {
        return clusterHasCapability(client, "POST", "/_query", List.of(), REQUIRED_CAPABILITIES).orElse(false);
    }

    private static void createMetricsIndex(RestClient client) throws IOException {
        Request create = new Request("PUT", "/" + INDEX);
        create.setJsonEntity("""
            {
              "settings": {
                "index.mode": "time_series",
                "index.routing_path": ["cluster", "host"],
                "index.number_of_shards": 1,
                "index.number_of_replicas": 0,
                "index.time_series.start_time": "2024-05-10T00:00:00Z",
                "index.time_series.end_time": "2024-05-10T01:00:00Z"
              },
              "mappings": {
                "properties": {
                  "@timestamp": {"type": "date"},
                  "cluster": {"type": "keyword", "time_series_dimension": true},
                  "host": {"type": "keyword", "time_series_dimension": true},
                  "rack": {"type": "long", "time_series_dimension": true},
                  "tx": {"type": "double", "time_series_metric": "gauge"}
                }
              }
            }
            """);
        assertOK(client.performRequest(create));
        Request index = new Request("POST", "/" + INDEX + "/_doc");
        index.addParameter("refresh", "true");
        index.setJsonEntity("""
            {"@timestamp": "2024-05-10T00:01:00Z", "cluster": "prod", "host": "a", "rack": 7, "tx": 10.0}
            """);
        assertOK(client.performRequest(index));
    }

    private static void assertMetadata(String index, String rack) throws IOException {
        String query = "PROMQL index="
            + index
            + " start=\""
            + QUERY_TIME
            + "\" end=\""
            + QUERY_TIME
            + "\" step=1m result=(tx) | KEEP result, _timeseries | LIMIT 10";
        Request request = new Request("POST", "/_query");
        request.setJsonEntity(Strings.toString(JsonXContent.contentBuilder().map(Map.of("query", query))));
        Map<String, Object> response = entityAsMap(client().performRequest(request));
        assertEquals(response.toString(), false, response.get("is_partial"));
        assertEquals(
            List.of(Map.of("name", "result", "type", "double"), Map.of("name", "_timeseries", "type", "keyword")),
            response.get("columns")
        );
        List<?> rows = (List<?>) response.get("values");
        assertEquals(1, rows.size());
        List<?> row = (List<?>) rows.getFirst();
        assertEquals(10.0, row.getFirst());
        Map<String, Object> metadata = XContentHelper.convertToMap(new BytesArray((String) row.get(1)), false, XContentType.JSON).v2();
        assertEquals(Map.of("cluster", "prod", "host", "a", "rack", rack), metadata);
    }

    private RestClient remoteClusterClient() throws IOException {
        HttpHost[] hosts = parseClusterHosts(remoteCluster.getHttpAddresses()).toArray(HttpHost[]::new);
        return buildClient(restClientSettings(), hosts);
    }
}
