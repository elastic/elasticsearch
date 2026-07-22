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
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.datasources.Federation;
import org.junit.ClassRule;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * End-to-end REST coverage for a node that boots with federation suppressed
 * ({@code -Des.esql.register_federation_feature=false}), the always-off deployment shape. Proves the six
 * data-source/dataset routes are unregistered: PUT, GET, and DELETE all return the framework's standard
 * {@code no handler found for uri} ({@code 400}), exactly as if the feature never existed. The switch is read
 * once at node startup, so this needs a dedicated cluster with the system property set on the node JVM (see the
 * {@code @ClassRule}).
 *
 * <p>Behavior against <em>pre-existing</em> federation state (executing {@code FROM <dataset>} against an existing
 * dataset) cannot be exercised here, because a boot-disabled node cannot create that state; it is covered by
 * {@link FederationKillSwitchRestartRestIT}, which creates state while enabled and then restarts the node with the
 * switch off. The complementary enabled-path CRUD coverage lives in {@link DataSourceCrudRestIT}.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class FederationDisabledRestIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster(spec -> spec.systemProperty(Federation.REGISTER_PROPERTY, "false"));

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testPutDataSourceRouteIsUnregistered() throws IOException {
        assertRouteUnregistered("PUT", "/_query/data_source/blocked_ds", "{\"type\":\"s3\",\"settings\":{\"auth\":\"anonymous\"}}");
    }

    public void testGetDataSourceRouteIsUnregistered() throws IOException {
        assertRouteUnregistered("GET", "/_query/data_source", null);
    }

    public void testDeleteDataSourceRouteIsUnregistered() throws IOException {
        assertRouteUnregistered("DELETE", "/_query/data_source/blocked_ds", null);
    }

    public void testPutDatasetRouteIsUnregistered() throws IOException {
        assertRouteUnregistered("PUT", "/_query/dataset/blocked_dataset", "{\"data_source\":\"some_parent\",\"resource\":\"s3://b/*\"}");
    }

    public void testGetDatasetRouteIsUnregistered() throws IOException {
        assertRouteUnregistered("GET", "/_query/dataset", null);
    }

    public void testDeleteDatasetRouteIsUnregistered() throws IOException {
        assertRouteUnregistered("DELETE", "/_query/dataset/blocked_dataset", null);
    }

    /**
     * Asserts the given route behaves like an endpoint that was never registered: HTTP 400 with a
     * {@code no handler found for uri} body. The body must not name the kill-switch property, so the feature
     * reads as absent rather than merely disabled.
     */
    private void assertRouteUnregistered(String method, String path, String jsonBody) throws IOException {
        Request req = new Request(method, path);
        if (jsonBody != null) {
            req.setJsonEntity(jsonBody);
        }
        ResponseException ex = expectThrows(ResponseException.class, () -> client().performRequest(req));
        String body = EntityUtils.toString(ex.getResponse().getEntity());
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(body, containsString("no handler found for uri"));
        assertThat(body, not(containsString(Federation.REGISTER_PROPERTY)));
    }
}
