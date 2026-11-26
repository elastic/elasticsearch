/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.datasources.datasource.RestTestDataSourceConnectionAction;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * Federation is available so data-source CRUD routes stay registered, but
 * {@link RestTestDataSourceConnectionAction#ESQL_DATA_SOURCE_TEST_CONNECTION_FEATURE_FLAG}
 * is forced off on the node JVM. The dedicated {@code POST /_query/data_source/_test} handler
 * is therefore absent. Because CRUD still registers {@code /_query/data_source/{name}}, a POST
 * to {@code /_query/data_source/_test} resolves as a wrong method on that template and returns
 * {@code 405}, not the federation-unavailable {@code 400 no handler found for uri}. CRUD itself
 * continues to work.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class DataSourceTestConnectionFeatureFlagOffRestIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster(
        // Snapshot defaults the FeatureFlag on; release defaults it off. Pin false so both builds
        // exercise the unregistered-_test / registered-CRUD split.
        spec -> spec.systemProperty("es.esql_data_source_test_connection_feature_flag_enabled", "false")
    );

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testTestConnectionRouteIsUnregistered() throws IOException {
        Request req = new Request("POST", "/_query/data_source/_test");
        req.setJsonEntity("{\"type\":\"s3\",\"settings\":{\"auth\":\"anonymous\"}}");
        ResponseException ex = expectThrows(ResponseException.class, () -> client().performRequest(req));
        String body = EntityUtils.toString(ex.getResponse().getEntity());
        // Path still matches CRUD's /_query/data_source/{name}; POST is not among its methods.
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(405));
        assertThat(body, containsString("Incorrect HTTP method"));
        assertThat(body, containsString("allowed: [GET, PUT, DELETE]"));
        // Must not look like a probe result (tri-state status values from the _test API).
        assertThat(body, not(containsString("\"status\":\"success\"")));
        assertThat(body, not(containsString("\"status\":\"failure\"")));
        assertThat(body, not(containsString("\"status\":\"untestable\"")));
    }

    public void testTestConnectionCapabilityNotAdvertised() throws IOException {
        Request caps = new Request(
            "GET",
            "/_capabilities?method=POST&path=/_query/data_source/_test&capabilities=data_source_test_connection"
        );
        Map<String, Object> response = entityAsMap(client().performRequest(caps));
        assertThat(response.get("supported"), equalTo(false));
    }

    public void testDataSourceCrudStillRegistered() throws IOException {
        Response response = client().performRequest(new Request("GET", "/_query/data_source"));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
    }
}
