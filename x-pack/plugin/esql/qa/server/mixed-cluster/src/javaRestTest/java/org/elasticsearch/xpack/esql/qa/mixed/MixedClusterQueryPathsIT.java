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
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

/**
 * {@code _xpack/usage} is built on whichever node receives it. Upgraded nodes only send their {@code query_paths} counters to
 * nodes that know how to turn them into the list of paths, so an old node never reports them in another shape.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class MixedClusterQueryPathsIT extends ESRestTestCase {
    private static final int OLD_NODE = 0;
    private static final int CURRENT_NODE = 1;

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.mixedVersionCluster();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @SuppressWarnings("unchecked")
    public void testOldNodesNeverReportQueryPaths() throws IOException {
        try (RestClient current = nodeClient(CURRENT_NODE); RestClient old = nodeClient(OLD_NODE)) {
            runKibanaQuery(current);
            runKibanaQuery(old);

            Map<String, Object> currentUsage = esqlUsage(current);
            assertThat(currentUsage.get("query_paths"), instanceOf(List.class));
            assertThat(
                (List<Object>) currentUsage.get("query_paths"),
                hasItem(
                    Map.of(
                        "caller",
                        "kibana",
                        "source",
                        "other_kibana",
                        "leading_command",
                        "row",
                        "total",
                        1,
                        "failed",
                        0,
                        "commands",
                        Map.of("row", 1, "limit", 1)
                    )
                )
            );

            assertThat(esqlUsage(old), not(hasKey("query_paths")));
        }
    }

    private RestClient nodeClient(int node) throws IOException {
        return buildClient(restClientSettings(), new HttpHost[] { HttpHost.create(cluster.getHttpAddress(node)) });
    }

    private static void runKibanaQuery(RestClient client) throws IOException {
        Request request = new Request("POST", "/_query");
        request.setJsonEntity("{\"query\": \"ROW a = 1 | LIMIT 1\"}");
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("X-elastic-product-origin", "kibana"));
        client.performRequest(request);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> esqlUsage(RestClient client) throws IOException {
        return (Map<String, Object>) entityAsMap(client.performRequest(new Request("GET", "/_xpack/usage"))).get("esql");
    }
}
