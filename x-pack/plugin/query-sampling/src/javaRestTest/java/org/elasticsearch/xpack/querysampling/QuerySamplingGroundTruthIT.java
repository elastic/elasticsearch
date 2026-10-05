/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling;

import org.elasticsearch.client.Request;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

/**
 * Runs the whole path against a real cluster: a kNN search is captured and sampled, then its ground truth is
 * computed through the REST API. A sampled query reaches the buffer on another thread, which a YAML test cannot
 * wait for, hence this class and its polling.
 */
public class QuerySamplingGroundTruthIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("x-pack-query-sampling")
        .feature(FeatureFlag.QUERY_SAMPLING)
        .setting("xpack.security.enabled", "false")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testGroundTruthOfASampledSearchIsComputed() throws Exception {
        createIndex("vectors", indexSettings(1, 0).build(), """
            "properties": { "vec": { "type": "dense_vector", "dims": 2, "index": true, "similarity": "l2_norm" } }
            """);
        for (int i = 0; i < 5; i++) {
            Request index = new Request("PUT", "/vectors/_doc/" + i);
            index.setJsonEntity("{\"vec\": [" + i + ", " + i + "]}");
            client().performRequest(index);
        }
        client().performRequest(new Request("POST", "/vectors/_refresh"));

        Request settings = new Request("PUT", "/_cluster/settings");
        settings.setJsonEntity("""
            { "persistent": { "xpack.query_sampling.enabled": true, "xpack.query_sampling.capture_rate": 1.0 } }
            """);
        client().performRequest(settings);

        // the node keeps what it sampled between tests, so this query must be new to it and counts are relative
        long buffered = nodeStat("buffered");
        long withGroundTruth = nodeStat("with_ground_truth");
        Request search = new Request("POST", "/vectors/_search");
        search.setJsonEntity(
            "{ \"knn\": { \"field\": \"vec\", \"query_vector\": [" + randomFloat() + ", 1.0], \"k\": 3, \"num_candidates\": 10 } }"
        );

        // the sampler picks a new query with a probability below one, so repeat the search until it did
        assertBusy(() -> {
            client().performRequest(search);
            assertThat(nodeStat("buffered"), equalTo(buffered + 1));
        });
        assertThat(nodeStat("with_ground_truth"), equalTo(withGroundTruth));

        ObjectPath result = ObjectPath.createFromResponse(client().performRequest(new Request("POST", "/_query_sampling/ground_truth")));
        assertThat(nodeValue(result, "computed"), equalTo(1L));
        assertThat(nodeValue(result, "failed"), equalTo(0L));
        assertThat(nodeStat("with_ground_truth"), equalTo(withGroundTruth + 1));

        // nothing is pending any more
        result = ObjectPath.createFromResponse(client().performRequest(new Request("POST", "/_query_sampling/ground_truth")));
        assertThat(nodeValue(result, "computed"), equalTo(0L));
    }

    private static long nodeStat(String name) throws IOException {
        return nodeValue(ObjectPath.createFromResponse(client().performRequest(new Request("GET", "/_query_sampling/stats"))), name);
    }

    private static long nodeValue(ObjectPath response, String name) throws IOException {
        Map<?, ?> nodes = response.evaluate("nodes");
        assertThat("the test cluster has one node", nodes.size(), equalTo(1));
        String nodeId = (String) nodes.keySet().iterator().next();
        return ((Number) response.evaluate("nodes." + nodeId + "." + name)).longValue();
    }
}
