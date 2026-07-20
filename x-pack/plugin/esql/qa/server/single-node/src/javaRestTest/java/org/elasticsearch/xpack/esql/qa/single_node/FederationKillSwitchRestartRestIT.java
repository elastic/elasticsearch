/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.datasources.Federation;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

/**
 * End-to-end REST coverage for engaging the federation kill switch on a live deployment: a node boots with
 * federation enabled, a data source and dataset are created, then the node is restarted with
 * {@code -Des.esql.federation.enabled=false}. This is the realistic "flip the switch and bounce" flow (the switch
 * is read once at startup), and it is the only way to observe the disabled behavior against <em>pre-existing</em>
 * federation state, which a boot-disabled cluster cannot create.
 *
 * <p>The property is supplied to the node JVM through a mutable holder ({@link #federationEnabled}); the test
 * flips it and calls {@link ElasticsearchCluster#restart(boolean)}, which re-reads the supplier. After the restart
 * the REST client is rebuilt because the node's ports may change.
 *
 * <p>Asserted after the switch is engaged:
 * <ul>
 *   <li>creating a dataset on the existing data source fails with 403 (create path dead);</li>
 *   <li>creating a new data source fails with 403;</li>
 *   <li>executing {@code FROM <dataset>} against the pre-existing dataset fails with 403;</li>
 *   <li>DELETE of the pre-existing dataset and data source still returns 200, and a subsequent GET returns
 *       nothing (the carve-out lets an operator clean up while the switch is engaged).</li>
 * </ul>
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class FederationKillSwitchRestartRestIT extends ESRestTestCase {

    private static final AtomicReference<String> federationEnabled = new AtomicReference<>("true");

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster(
        spec -> spec.systemProperty(Federation.ENABLED_PROPERTY, federationEnabled::get)
    );

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        // The cluster is restarted mid-test and is ephemeral for this class; skip the shared-cluster wipe.
        return true;
    }

    @BeforeClass
    public static void disableForReleaseBuilds() {
        assumeTrue("datasources not available in release builds yet", Build.current().isSnapshot());
    }

    public void testKillSwitchEngagesAfterRestart() throws Exception {
        final String source = "restart_source";
        final String dataset = "restart_dataset";

        // Phase 1: federation enabled. Create the state the disabled phase will act on.
        putDataSource(source, Map.of("region", "us-east-1", "auth", "anonymous"));
        putDataset(dataset, source, "s3://bucket/x/*.parquet");
        assertThat(datasourceNames(), contains(source));
        assertThat(datasetNames(), contains(dataset));

        // Engage the kill switch and bounce the node so it reads the new value.
        federationEnabled.set("false");
        cluster.restart(false);
        closeClients();
        initClient();

        // GET still works and the pre-existing state survived the restart (also proves the read carve-out).
        assertThat(datasourceNames(), contains(source));
        assertThat(datasetNames(), contains(dataset));

        // Create paths are dead: a dataset on the existing source, and a brand-new source, both 403.
        assertForbidden(expectThrows(ResponseException.class, () -> putDataset("late_dataset", source, "s3://bucket/y/*.parquet")));
        assertForbidden(
            expectThrows(ResponseException.class, () -> putDataSource("late_source", Map.of("region", "us-east-1", "auth", "anonymous")))
        );

        // Executing FROM <dataset> against the pre-existing dataset is blocked.
        Request query = new Request("POST", "/_query");
        query.setJsonEntity("{\"query\": \"FROM " + dataset + "\"}");
        assertForbidden(expectThrows(ResponseException.class, () -> client().performRequest(query)));

        // DELETE of the pre-existing dataset and source still works, and GET afterward returns nothing.
        assertOkDelete("dataset", dataset);
        assertThat(datasetNames(), empty());
        assertOkDelete("data_source", source);
        assertThat(datasourceNames(), empty());
    }

    private static void assertForbidden(ResponseException ex) throws IOException {
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(403));
        assertThat(EntityUtils.toString(ex.getResponse().getEntity()), containsString(Federation.ENABLED_PROPERTY));
    }

    private static void assertOkDelete(String kind, String name) throws IOException {
        Response r = client().performRequest(new Request("DELETE", "/_query/" + kind + "/" + name));
        assertThat(r.getStatusLine().getStatusCode(), equalTo(200));
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
