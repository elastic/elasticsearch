/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.grpc;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.datasource.grpc.BlockingSatelliteServer;
import org.elasticsearch.xpack.esql.datasources.Federation;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.notNullValue;

/**
 * A query cancelled while the satellite is still computing its first batch must reach the satellite: the call it is serving is
 * cancelled and the query's task goes away. The satellite runs in this JVM and stalls on purpose, so no external process is needed.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class SatelliteCancellationIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .nodes(2)
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting(Federation.FEDERATION_ENABLED.getKey(), "true")
        .setting("xpack.ml.enabled", "false")
        .jvmArg("--add-opens=java.base/java.nio=ALL-UNNAMED")
        .jvmArg("-Darrow.allocation.manager.type=Unsafe")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testDeletingARunningQueryCancelsTheSatelliteCall() throws Exception {
        try (BlockingSatelliteServer satellite = new BlockingSatelliteServer()) {
            satellite.blockInDoGet = true;
            Request source = new Request("PUT", "/_query/data_source/cancel_src");
            source.setJsonEntity("{\"type\": \"flight\"}");
            client().performRequest(source);
            Request dataset = new Request("PUT", "/_query/dataset/cancel_ds");
            dataset.setJsonEntity("{\"data_source\": \"cancel_src\", \"resource\": \"flight://127.0.0.1:" + satellite.port() + "/t\"}");
            client().performRequest(dataset);
            try {
                Request query = new Request("POST", "/_query/async");
                query.setJsonEntity("{\"query\": \"FROM cancel_ds | KEEP id | LIMIT 5\", \"wait_for_completion_timeout\": \"1ms\"}");
                Map<String, Object> response = entityAsMap(client().performRequest(query));
                String id = (String) response.get("id");
                assertThat("the query should still be running", id, notNullValue());
                assertTrue(
                    "the satellite never got the read; response " + response + ", fragments " + satellite.fragments,
                    satellite.inDoGet.await(30, TimeUnit.SECONDS)
                );

                Response deleted = client().performRequest(new Request("DELETE", "/_query/async/" + id));
                assertEquals(200, deleted.getStatusLine().getStatusCode());

                assertTrue("the satellite must see its call cancelled", satellite.doGetCancelled.await(30, TimeUnit.SECONDS));
                assertFalse(satellite.servedRows.get());
            } finally {
                satellite.release.countDown();
                deleteIgnoring("/_query/dataset/cancel_ds");
                deleteIgnoring("/_query/data_source/cancel_src");
            }
        }
    }

    private void deleteIgnoring(String path) throws IOException {
        try {
            client().performRequest(new Request("DELETE", path));
        } catch (org.elasticsearch.client.ResponseException e) {
            // already gone
        }
    }
}
