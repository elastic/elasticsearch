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
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.datasources.Federation;
import org.elasticsearch.xpack.esql.datasources.S3BackendFixture;
import org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.DataSourcesS3HttpFixture;
import org.junit.ClassRule;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.WAREHOUSE;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * End-to-end REST coverage for turning federation off and back on on a live deployment, in three phases:
 * <ol>
 *   <li>a node boots with federation available; a data source and a dataset are created and the dataset is read;</li>
 *   <li>one of the two levers is turned off and the node is restarted: it must start, and the feature must look like it
 *       never existed even though the state from phase 1 is still in cluster state;</li>
 *   <li>the lever is turned back on and the node is restarted again: the state must still be there, and the dataset
 *       must be readable again, so that an operator who opts out and back in loses nothing.</li>
 * </ol>
 * This is the realistic "flip it and bounce" flow (both levers are read once at startup), and it is the only way to
 * observe the unavailable behavior against <em>pre-existing</em> federation state, which a cluster that boots without
 * federation cannot create.
 *
 * <p>Both levers must produce the same surface, so subclasses share this flow and differ only in
 * {@link #setFederation(boolean)}. Values reach the node through a mutable holder that the cluster spec reads on every
 * (re)start; after {@link ElasticsearchCluster#restart(boolean)} the REST client is rebuilt because the node's ports may
 * change.
 *
 * <p>The dataset is a CSV file on an in-process S3 fixture, so that phases 1 and 3 can actually read it. The fixture is
 * a class rule of this base, independent of the node, so its endpoint, registered in the data source in phase 1, is
 * still valid after both restarts. {@code s3} with a CSV file is used because neither carries a snapshot-only feature
 * flag and these suites also run against release builds; the data source carries the fixture's credentials, so the
 * encrypted secret in cluster state round-trips through a node that boots without the feature as well. The read is a
 * projection rather than an aggregation, because an ungrouped {@code COUNT}/{@code MIN}/{@code MAX} can be answered from
 * split statistics without ever reading the file.
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

    private static final String DATASET_PREFIX = WAREHOUSE + "/restart";
    private static final String CSV = """
        id:integer,name:keyword,value:integer
        1,name-1,10
        2,name-2,20
        3,name-3,30
        """;
    private static final List<List<Object>> EXPECTED_ROWS = List.of(
        List.of(1, "name-1", 10),
        List.of(2, "name-2", 20),
        List.of(3, "name-3", 30)
    );

    @ClassRule
    public static DataSourcesS3HttpFixture s3Fixture = new DataSourcesS3HttpFixture();

    /**
     * The suite's own cluster, which this flow restarts twice. Each subclass declares its own {@code @ClassRule} because
     * the two levers are supplied to the node differently.
     */
    protected abstract ElasticsearchCluster cluster();

    /**
     * Sets this suite's lever to on or off. The value is only picked up by the restart that follows. A subclass must
     * also keep {@code esql.external.allowed_endpoint_hosts} in step with the lever if its cluster does not carry it
     * permanently: the fixture is reached over plain {@code http}, which the {@code s3} data source only accepts for an
     * allowlisted host, and the allowlist is a federation setting that an unregistered node refuses to start with.
     */
    protected abstract void setFederation(boolean on);

    @Override
    protected boolean preserveClusterUponCompletion() {
        // The cluster is restarted mid-test and is ephemeral for this class; skip the shared-cluster wipe.
        return true;
    }

    public void testFeatureFollowsTheLeverAcrossRestarts() throws Exception {
        final String source = "restart_source";
        final String dataset = "restart_dataset";
        final S3BackendFixture backend = new S3BackendFixture(s3Fixture);

        // Phase 1: federation available. Create the state the other phases act on, and read it once as the baseline.
        backend.uploadBlob(DATASET_PREFIX + "/part-1.csv", CSV.getBytes(StandardCharsets.UTF_8));
        putDataSource(source, backend.dataSourceSettings());
        putDataset(dataset, source, backend.resourceUri(DATASET_PREFIX + "/*.csv"));
        assertThat(datasourceNames(), contains(source));
        assertThat(datasetNames(), contains(dataset));
        assertThat(queryRows(dataset), equalTo(EXPECTED_ROWS));

        // Phase 2: federation off. The node must start, and the feature must look absent.
        setFederation(false);
        restartAndReconnect();

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
        ResponseException ex = expectThrows(ResponseException.class, () -> client().performRequest(query(dataset)));
        String body = EntityUtils.toString(ex.getResponse().getEntity());
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(body, containsString("Unknown index [" + dataset + "]"));
        assertNoLeverNamed(body);

        // Phase 3: federation back on. The state survived both bounces, the routes are back, and the dataset is read
        // through the data source's stored endpoint, credentials and format configuration exactly as in phase 1.
        setFederation(true);
        restartAndReconnect();

        assertThat(datasourceNames(), contains(source));
        assertThat(datasetNames(), contains(dataset));
        assertThat(queryRows(dataset), equalTo(EXPECTED_ROWS));

        // The routes are back, so the state can be cleaned up over REST again.
        assertThat(
            client().performRequest(new Request("DELETE", "/_query/dataset/" + dataset)).getStatusLine().getStatusCode(),
            equalTo(200)
        );
        assertThat(
            client().performRequest(new Request("DELETE", "/_query/data_source/" + source)).getStatusLine().getStatusCode(),
            equalTo(200)
        );
    }

    private void restartAndReconnect() throws IOException {
        cluster().restart(false);
        closeClients();
        initClient();
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

    private static Request query(String dataset) {
        Request query = new Request("POST", "/_query");
        // An explicit LIMIT keeps the "adding default limit" warning away from the strict default warnings handler.
        query.setJsonEntity("{\"query\": \"FROM " + dataset + " | KEEP id, name, value | SORT id | LIMIT 10\"}");
        return query;
    }

    /** Reads every row of the dataset. A projection, so the file is actually read rather than answered from statistics. */
    @SuppressWarnings("unchecked")
    private static List<List<Object>> queryRows(String dataset) throws IOException {
        Response r = client().performRequest(query(dataset));
        assertThat(r.getStatusLine().getStatusCode(), equalTo(200));
        return (List<List<Object>>) entityAsMap(r).get("values");
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
        req.setOptions(req.getOptions().toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE).build());
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
