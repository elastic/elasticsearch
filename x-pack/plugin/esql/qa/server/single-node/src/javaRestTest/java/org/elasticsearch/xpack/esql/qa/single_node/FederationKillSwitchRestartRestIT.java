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
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.datasources.Federation;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * End-to-end REST coverage for suppressing federation on a live deployment: a node boots with federation enabled, a
 * data source and dataset are created, then the node is restarted with
 * {@code -Des.esql.register_federation_feature=false}. This is the realistic "flip the switch and bounce" flow (the
 * switch is read once at startup), and it is the only way to observe the suppressed behavior against
 * <em>pre-existing</em> federation state, which a boot-disabled cluster cannot create.
 *
 * <p>The property is supplied to the node JVM through a mutable holder ({@link #registerFederationFeature}); the test
 * flips it and calls {@link ElasticsearchCluster#restart(boolean)}, which re-reads the supplier. After the restart the
 * REST client is rebuilt because the node's ports may change.
 *
 * <p>Asserted after the switch is engaged (federation must look like it never existed):
 * <ul>
 *   <li>PUT, GET, and DELETE of data sources and datasets all return HTTP 400 {@code no handler found for uri} (the
 *       routes are unregistered), even though the pre-existing state is still in cluster state;</li>
 *   <li>executing {@code FROM <dataset>} against the pre-existing dataset fails as {@code Unknown index} (HTTP 400),
 *       the same error a nonexistent index gives, so the dataset is never resolved or accessed.</li>
 * </ul>
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class FederationKillSwitchRestartRestIT extends ESRestTestCase {

    private static final AtomicReference<String> registerFederationFeature = new AtomicReference<>("true");

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster(
        spec -> spec.systemProperty(Federation.REGISTER_PROPERTY, registerFederationFeature::get)
    );

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

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

        // Phase 1: federation enabled. Create the state the suppressed phase will act on.
        putDataSource(source, Map.of("region", "us-east-1", "auth", "anonymous"));
        putDataset(dataset, source, "s3://bucket/x/*.parquet");
        assertThat(datasourceNames(), contains(source));
        assertThat(datasetNames(), contains(dataset));

        // Suppress federation and bounce the node so it reads the new value.
        registerFederationFeature.set("false");
        cluster.restart(false);
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
        // index name gives. The dataset is not accessed and the kill-switch property must not leak into the message.
        Request query = new Request("POST", "/_query");
        query.setJsonEntity("{\"query\": \"FROM " + dataset + "\"}");
        ResponseException ex = expectThrows(ResponseException.class, () -> client().performRequest(query));
        String body = EntityUtils.toString(ex.getResponse().getEntity());
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(body, containsString("Unknown index [" + dataset + "]"));
        assertThat(body, not(containsString(Federation.REGISTER_PROPERTY)));
    }

    /**
     * Asserts the given route behaves like an endpoint that was never registered: HTTP 400 with a
     * {@code no handler found for uri} body that does not name the kill-switch property.
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
        assertThat(body, not(containsString(Federation.REGISTER_PROPERTY)));
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
