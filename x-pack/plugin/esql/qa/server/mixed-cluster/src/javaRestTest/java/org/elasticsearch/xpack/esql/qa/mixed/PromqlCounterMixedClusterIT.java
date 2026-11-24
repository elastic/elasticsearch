/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.mixed;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * PromQL {@code count_over_time} of a counter in a cluster where only some nodes accept a counter in {@code COUNT}. Each
 * node coordinates the query once and counts every series, whichever nodes hold the shards.
 * <p>
 * The index has no replicas, so a shard on a node that can't run the query has no other copy to fall back to. In a
 * stateless cluster such an index has no search shard, so the test skips itself there.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class PromqlCounterMixedClusterIT extends ESRestTestCase {
    private static final String INDEX = "promql_counter_mixed";
    private static final int SERIES = 32;
    private static final String QUERY = "PROMQL index=" + INDEX + " step=1m samples=(sum(count_over_time(requests[1m])))";

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.mixedVersionCluster();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testEveryCoordinatorCountsEverySeries() throws IOException {
        List<String> required = List.of(
            EsqlCapabilities.Cap.PROMQL_COMMAND_V0.capabilityName(),
            EsqlCapabilities.Cap.FIX_PROMQL_TIME_BUCKET_V2.capabilityName(),
            EsqlCapabilities.Cap.TO_GAUGE.capabilityName()
        );
        assumeTrue(
            required + " not supported by all nodes in this mixed-version cluster",
            clusterHasCapability("POST", "/_query", List.of(), required).orElse(false)
        );
        assumeFalse("the cluster is stateless", isStateless());
        createIndex();
        for (HttpHost host : getClusterHosts()) {
            try (RestClient coordinator = buildClient(restClientSettings(), new HttpHost[] { host })) {
                Request request = new Request("POST", "/_query");
                request.setJsonEntity("{\"query\": \"" + QUERY + "\"}");
                List<List<Object>> values = ObjectPath.createFromResponse(coordinator.performRequest(request)).evaluate("values");
                assertThat(host.toString(), values, hasSize(1));
                assertThat(host.toString(), values.getFirst().getFirst(), equalTo((double) SERIES));
            }
        }
    }

    private void createIndex() throws IOException {
        Request create = new Request("PUT", "/" + INDEX);
        create.setJsonEntity("""
            {
              "settings": {
                "index": {
                  "mode": "time_series",
                  "routing_path": ["pod"],
                  "number_of_shards": 8,
                  "number_of_replicas": 0,
                  "time_series": {"start_time": "2021-04-28T00:00:00Z", "end_time": "2021-04-29T00:00:00Z"}
                }
              },
              "mappings": {
                "properties": {
                  "@timestamp": {"type": "date"},
                  "pod": {"type": "keyword", "time_series_dimension": true},
                  "requests": {"type": "long", "time_series_metric": "counter"}
                }
              }
            }""");
        client().performRequest(create);
        StringBuilder bulk = new StringBuilder();
        for (int series = 0; series < SERIES; series++) {
            bulk.append("{\"index\":{}}\n")
                .append(
                    Strings.format("{\"@timestamp\": \"2021-04-28T18:50:30Z\", \"pod\": \"pod-%s\", \"requests\": %s}\n", series, series)
                );
        }
        Request request = new Request("POST", "/" + INDEX + "/_bulk");
        request.addParameter("refresh", "true");
        request.setJsonEntity(bulk.toString());
        assertThat(ObjectPath.createFromResponse(client().performRequest(request)).evaluate("errors"), equalTo(false));
    }

    /**
     * Whether the cluster is stateless: some node has the role of indexing or searching shards.
     */
    private static boolean isStateless() throws IOException {
        ObjectPath info = ObjectPath.createFromResponse(client().performRequest(new Request("GET", "/_nodes")));
        Map<String, Object> nodes = info.evaluate("nodes");
        for (String id : nodes.keySet()) {
            List<String> roles = info.evaluate("nodes." + id + ".roles");
            if (roles.contains(DiscoveryNodeRole.INDEX_ROLE.roleName()) || roles.contains(DiscoveryNodeRole.SEARCH_ROLE.roleName())) {
                return true;
            }
        }
        return false;
    }
}
