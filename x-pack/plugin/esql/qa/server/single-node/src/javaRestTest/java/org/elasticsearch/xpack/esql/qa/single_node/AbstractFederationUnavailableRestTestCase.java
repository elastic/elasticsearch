/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.datasources.Federation;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * REST coverage shared by every deployment shape in which federation is not available: the six data source and dataset
 * routes must be unregistered, so PUT, GET and DELETE all return the framework's standard
 * {@code no handler found for uri} ({@code 400}), exactly as if the feature never existed, and no response names either
 * of the two levers. Subclasses differ only in which lever they turn off.
 *
 * <p>Behavior against <em>pre-existing</em> federation state (executing {@code FROM <dataset>} against an existing
 * dataset) cannot be exercised here, because a node that boots without federation cannot create that state; it is
 * covered by {@link FederationKillSwitchRestartRestIT} and {@link FederationSettingRestartRestIT}, which create state
 * while enabled and then restart the node with one lever off. The complementary enabled-path CRUD coverage lives in
 * {@link DataSourceCrudRestIT}.
 */
public abstract class AbstractFederationUnavailableRestTestCase extends ESRestTestCase {

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
     * A name that would be a dataset on a cluster with federation reads as a plain missing index here. No dataset can
     * exist on this cluster (the routes that create one are gone), so this pins the error shape rather than the gate
     * itself: the message must be the ordinary {@code Unknown index} and must not mention datasets or either lever.
     */
    public void testFromDatasetNameLooksLikeMissingIndex() throws IOException {
        Request query = new Request("POST", "/_query");
        query.setJsonEntity("{\"query\": \"FROM blocked_dataset\"}");
        ResponseException ex = expectThrows(ResponseException.class, () -> client().performRequest(query));
        String body = EntityUtils.toString(ex.getResponse().getEntity());
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(body, containsString("Unknown index [blocked_dataset]"));
        assertNoLeverNamed(body);
    }

    /**
     * Asserts the given route behaves like an endpoint that was never registered: HTTP 400 with a
     * {@code no handler found for uri} body. The body must not name either lever, so the feature reads as absent
     * rather than merely disabled.
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
        assertNoLeverNamed(body);
    }

    private static void assertNoLeverNamed(String body) {
        assertThat(body, not(containsString(Federation.REGISTER_PROPERTY)));
        assertThat(body, not(containsString(Federation.FEDERATION_ENABLED.getKey())));
    }
}
