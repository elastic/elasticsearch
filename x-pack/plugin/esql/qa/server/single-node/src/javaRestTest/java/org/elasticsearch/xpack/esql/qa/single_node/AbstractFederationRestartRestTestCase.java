/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.datasources.Federation;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * End-to-end REST coverage for turning federation off on a live deployment: a node boots with federation available, a
 * data source and dataset are created, then one of the two levers is turned off and the node is restarted. This is the
 * realistic "flip it and bounce" flow (both levers are read once at startup), and it is the only way to observe the
 * unavailable behavior against <em>pre-existing</em> federation state, which a cluster that boots without federation
 * cannot create.
 *
 * <p>Both levers must produce the same surface, so subclasses share this flow and differ only in
 * {@link #turnFederationOff()}. Values reach the node through a mutable holder that the cluster spec reads on every
 * (re)start; after {@link ElasticsearchCluster#restart(boolean)} the REST client is rebuilt because the node's ports may
 * change.
 *
 * <p>Asserted after federation is off (it must look like it never existed):
 * <ul>
 *   <li>PUT, GET, and DELETE of data sources and datasets all return HTTP 400 {@code no handler found for uri} (the
 *       routes are unregistered), even though the pre-existing state is still in cluster state;</li>
 *   <li>executing {@code FROM <dataset>} against the pre-existing dataset fails as {@code Unknown index} (HTTP 400),
 *       the same error a nonexistent index gives, so the dataset is never resolved or accessed.</li>
 * </ul>
 */
public abstract class AbstractFederationRestartRestTestCase extends ESRestTestCase {

    /**
     * The suite's own cluster, which this flow restarts. Each subclass declares its own {@code @ClassRule} because the
     * two levers are supplied to the node differently.
     */
    protected abstract ElasticsearchCluster cluster();

    /**
     * Sets this suite's lever to off. The value is only picked up by the restart that follows.
     */
    protected abstract void turnFederationOff();

    @Override
    protected boolean preserveClusterUponCompletion() {
        // The cluster is restarted mid-test and is ephemeral for this class; skip the shared-cluster wipe. The
        // federation routes are unregistered after the restart, so the created state cannot be cleaned up over REST
        // anyway.
        return true;
    }

    public void testFeatureLooksAbsentAfterRestart() throws Exception {
        final String source = "restart_source";
        final String dataset = "restart_dataset";

        // Phase 1: federation available. Create the state the unavailable phase will act on.
        putDataSource(source, Map.of("region", "us-east-1", "auth", "anonymous"));
        putDataset(dataset, source, "s3://bucket/x/*.parquet");
        assertThat(datasourceNames(), contains(source));
        assertThat(datasetNames(), contains(dataset));

        turnFederationOff();
        cluster().restart(false);
        closeClients();
        initClient();

        // All six routes are gone: PUT/GET/DELETE for both data sources and datasets return the standard no-handler 400,
        // even though the pre-existing state is still in cluster state.
        assertRouteUnregistered("PUT", "/_query/data_source/late_source", "{\"type\":\"s3\",\"settings\":{\"auth\":\"anonymous\"}}");
        assertRouteUnregistered("GET", "/_query/data_source", null);
        assertRouteUnregistered("DELETE", "/_query/data_source/" + source, null);
        assertRouteUnregistered("PUT", "/_query/dataset/late_dataset", "{\"data_source\":\"" + source + "\",\"resource\":\"s3://b/*\"}");
        assertRouteUnregistered("GET", "/_query/dataset", null);
        assertRouteUnregistered("DELETE", "/_query/dataset/" + dataset, null);

        // FROM <pre-existing dataset> is never resolved as a dataset: the name flows into normal index resolution with
        // resolveDatasets=false, so it matches no concrete index and errors as Unknown index, the same 400 a nonexistent
        // index name gives. The dataset is not accessed and neither lever must leak into the message.
        Request query = new Request("POST", "/_query");
        query.setJsonEntity("{\"query\": \"FROM " + dataset + "\"}");
        ResponseException ex = expectThrows(ResponseException.class, () -> client().performRequest(query));
        String body = EntityUtils.toString(ex.getResponse().getEntity());
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(body, containsString("Unknown index [" + dataset + "]"));
        assertNoLeverNamed(body);
    }

    /**
     * Asserts the given route behaves like an endpoint that was never registered: HTTP 400 with a
     * {@code no handler found for uri} body that does not name either lever.
     */
    private static void assertRouteUnregistered(String method, String path, String jsonBody) throws IOException {
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

    @SuppressWarnings("unchecked")
    private static List<String> datasourceNames() throws IOException {
        Response r = client().performRequest(new Request("GET", "/_query/data_source"));
        assertThat(r.getStatusLine().getStatusCode(), equalTo(200));
        return ((List<Map<String, Object>>) entityAsMap(r).get("data_sources")).stream().map(h -> (String) h.get("name")).toList();
    }

    @SuppressWarnings("unchecked")
    private static List<String> datasetNames() throws IOException {
        Response r = client().performRequest(new Request("GET", "/_query/dataset"));
        assertThat(r.getStatusLine().getStatusCode(), equalTo(200));
        return ((List<Map<String, Object>>) entityAsMap(r).get("datasets")).stream().map(h -> (String) h.get("name")).toList();
    }

    private static void putDataSource(String name, Map<String, Object> settings) throws IOException {
        Request req = new Request("PUT", "/_query/data_source/" + name);
        try (XContentBuilder b = jsonBuilder()) {
            b.startObject().field("type", "s3").field("settings", settings).endObject();
            req.setJsonEntity(Strings.toString(b));
        }
        assertThat(client().performRequest(req).getStatusLine().getStatusCode(), equalTo(200));
    }

    private static void putDataset(String name, String dataSource, String resource) throws IOException {
        Request req = new Request("PUT", "/_query/dataset/" + name);
        try (XContentBuilder b = jsonBuilder()) {
            b.startObject().field("data_source", dataSource).field("resource", resource).endObject();
            req.setJsonEntity(Strings.toString(b));
        }
        assertThat(client().performRequest(req).getStatusLine().getStatusCode(), equalTo(200));
    }
}
