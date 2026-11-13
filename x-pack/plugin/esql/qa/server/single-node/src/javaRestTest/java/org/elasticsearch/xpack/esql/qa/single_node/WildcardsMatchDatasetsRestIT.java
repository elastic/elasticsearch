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
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

/**
 * End-to-end REST coverage for the {@code wildcards_match_datasets} query setting, against the scenario reported on a 9.5.2
 * cluster in elastic/elasticsearch#158472: enough registered datasets make every {@code FROM *} fail, because a
 * wildcard sweeps them all in and each becomes its own plan branch.
 *
 * <p>This is the layer that can register a data source, so it sits beside {@link DataSourceCrudRestIT} rather than in
 * the {@code x-pack:plugin:esql} unit suites.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class WildcardsMatchDatasetsRestIT extends ESRestTestCase {

    private static final int DATASET_COUNT = 8;
    private static final String INDEX = "logs-000001";
    private static final String DATA_SOURCE = "lake";

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @BeforeClass
    public static void disableForReleaseBuilds() {
        assumeTrue("datasources not available in release builds yet", Build.current().isSnapshot());
    }

    @Before
    public void registerIndexAndDatasets() throws IOException {
        Request doc = new Request("PUT", "/" + INDEX + "/_doc/1");
        doc.addParameter("refresh", "true");
        doc.setJsonEntity("{\"message\": \"hello\"}");
        client().performRequest(doc);

        putDataSource(DATA_SOURCE);
        for (int i = 1; i <= DATASET_COUNT; i++) {
            putDataset(DATA_SOURCE + "_" + i, "s3://bucket/" + i + "/*.csv");
        }
    }

    public void testWildcardDoesNotMatchDatasetsByDefault() throws IOException {
        // The default: the wildcard means index-likes, so the registered datasets neither contribute branches nor are
        // read. The query returns what FROM logs-000001 returns. KEEP pins the projection because the dynamic mapping
        // gives `message` a `.keyword` sub-field, and the explicit LIMIT keeps the default-limit warning off the wire.
        Map<String, Object> response = query("FROM * | KEEP message | LIMIT 10");

        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) response.get("values");
        assertThat(values, hasSize(1));
        assertThat(values.get(0), equalTo(List.of("hello")));
    }

    public void testWildcardReachesDatasetOnlyWhenSettingIsOn() throws IOException {
        // lake_1* matches exactly one dataset and no index, so this pins REACH itself rather than any downstream
        // threshold: the branch cap is nowhere near, and raising it could not affect this case.

        // Off (the default): the wildcard matches nothing at all, so the query succeeds with no rows. The dataset was
        // never reached -- had it been, resolving its object-storage resource would have failed the query.
        Map<String, Object> off = query("FROM lake_1* | LIMIT 1");
        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) off.get("values");
        assertThat(values, empty());

        // On: the same wildcard now reaches the dataset and the query fails trying to read its resource. That failure
        // IS the proof of reach -- the bucket returns a storage error, which is exactly why it is unambiguous.
        // The error must not expose the bucket name (storage location is hidden from all users per the security model).
        ResponseException ex = expectThrows(
            ResponseException.class,
            () -> query("SET wildcards_match_datasets = true; FROM lake_1* | LIMIT 1")
        );
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        String body = EntityUtils.toString(ex.getResponse().getEntity());
        // The query reached the dataset and attempted a listing — the listing failure proves reach.
        // The bucket name must not appear: storage location is hidden from all users per the security model.
        assertThat(body, containsString("Access denied listing objects"));
        assertThat(body, not(containsString("s3://bucket")));
    }

    public void testDisabledDatasetIsRefusedByName() throws IOException {
        toggle("dataset", DATA_SOURCE + "_1", "_disable");
        try {
            ResponseException ex = expectThrows(ResponseException.class, () -> query("FROM " + DATA_SOURCE + "_1 | LIMIT 1"));
            assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(400));
            assertThat(EntityUtils.toString(ex.getResponse().getEntity()), containsString("dataset [lake_1] is disabled"));
        } finally {
            toggle("dataset", DATA_SOURCE + "_1", "_enable");
        }
        // Enabled again, the dataset is reached again: the query gets as far as listing its resource.
        ResponseException ex = expectThrows(ResponseException.class, () -> query("FROM " + DATA_SOURCE + "_1 | LIMIT 1"));
        assertThat(EntityUtils.toString(ex.getResponse().getEntity()), containsString("Access denied listing objects"));
    }

    public void testDisabledDataSourceRefusesItsDatasets() throws IOException {
        toggle("data_source", DATA_SOURCE, "_disable");
        try {
            ResponseException ex = expectThrows(ResponseException.class, () -> query("FROM " + DATA_SOURCE + "_1 | LIMIT 1"));
            assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(400));
            String body = EntityUtils.toString(ex.getResponse().getEntity());
            assertThat(body, containsString("dataset [lake_1] is disabled because its data source is disabled"));
            assertThat("the data source is not named", body, not(containsString("[lake]")));
        } finally {
            toggle("data_source", DATA_SOURCE, "_enable");
        }
    }

    /**
     * A wildcard that reaches a disabled dataset skips it, with a warning, instead of failing the query: an admin switch must
     * not break every query whose pattern happens to cover the dataset. Naming the dataset still fails, as above.
     */
    public void testWildcardSkipsDisabledDataset() throws IOException {
        toggle("dataset", DATA_SOURCE + "_1", "_disable");
        try {
            // lake_1* matches only the disabled dataset: nothing is left to read, so the query succeeds with no rows.
            Map<String, Object> response = query(
                "SET wildcards_match_datasets = true; FROM lake_1* | LIMIT 1",
                "a wildcard in FROM skipped the disabled datasets [lake_1]"
            );
            @SuppressWarnings("unchecked")
            List<List<Object>> values = (List<List<Object>>) response.get("values");
            assertThat(values, empty());

            // lake_* still reaches the enabled datasets: the query gets as far as listing their resources.
            ResponseException ex = expectThrows(
                ResponseException.class,
                () -> query("SET wildcards_match_datasets = true; FROM lake_* | LIMIT 1")
            );
            assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(400));
            assertThat(EntityUtils.toString(ex.getResponse().getEntity()), containsString("Access denied listing objects"));
        } finally {
            toggle("dataset", DATA_SOURCE + "_1", "_enable");
        }
    }

    public void testWildcardMatchingDatasetsRespectsBranchCountLimit() throws IOException {
        // Lower the cap below DATASET_COUNT so expanding lake_* to all registered datasets triggers the protection.
        int lowCap = DATASET_COUNT - 2;
        Request lowerCap = new Request("PUT", "/_cluster/settings");
        lowerCap.setJsonEntity(Strings.format("""
            {"persistent": {"esql.query.max_branch_count_per_merge": %d}}
            """, lowCap));
        assertOK(client().performRequest(lowerCap));
        try {
            ResponseException ex = expectThrows(
                ResponseException.class,
                () -> query("SET wildcards_match_datasets = true; FROM lake_* | LIMIT 1")
            );
            assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(400));
            String body = EntityUtils.toString(ex.getResponse().getEntity());
            assertThat(body, containsString("FROM [lake_*] resolved to " + DATASET_COUNT + " branches, exceeding the limit of " + lowCap));
        } finally {
            Request cleanup = new Request("PUT", "/_cluster/settings");
            cleanup.setJsonEntity("""
                {"persistent": {"esql.query.max_branch_count_per_merge": null}}
                """);
            client().performRequest(cleanup);
        }
    }

    private static Map<String, Object> query(String esql) throws IOException {
        Request req = new Request("POST", "/_query");
        try (XContentBuilder b = jsonBuilder()) {
            b.startObject().field("query", esql).endObject();
            req.setJsonEntity(Strings.toString(b));
        }
        Response resp = client().performRequest(req);
        return entityAsMap(resp);
    }

    /** As {@link #query(String)}, for a query that is expected to answer with exactly these warnings. */
    private static Map<String, Object> query(String esql, String... expectedWarnings) throws IOException {
        Request req = new Request("POST", "/_query");
        try (XContentBuilder b = jsonBuilder()) {
            b.startObject().field("query", esql).endObject();
            req.setJsonEntity(Strings.toString(b));
        }
        req.setOptions(
            RequestOptions.DEFAULT.toBuilder().setWarningsHandler(warnings -> warnings.equals(List.of(expectedWarnings)) == false)
        );
        return entityAsMap(client().performRequest(req));
    }

    /** {@code kind} is {@code dataset} or {@code data_source}; {@code op} is {@code _enable} or {@code _disable}. */
    private static void toggle(String kind, String name, String op) throws IOException {
        Response r = client().performRequest(new Request("POST", "/_query/" + kind + "/" + name + "/" + op));
        assertThat(r.getStatusLine().getStatusCode(), equalTo(200));
    }

    private static void putDataSource(String name) throws IOException {
        Request req = new Request("PUT", "/_query/data_source/" + name);
        try (XContentBuilder b = jsonBuilder()) {
            b.startObject().field("type", "s3").field("settings", Map.of("auth", "anonymous")).endObject();
            req.setJsonEntity(Strings.toString(b));
        }
        Response r = client().performRequest(req);
        assertThat(r.getStatusLine().getStatusCode(), equalTo(200));
    }

    private static void putDataset(String name, String resource) throws IOException {
        Request req = new Request("PUT", "/_query/dataset/" + name);
        try (XContentBuilder b = jsonBuilder()) {
            b.startObject().field("data_source", DATA_SOURCE).field("resource", resource).endObject();
            req.setJsonEntity(Strings.toString(b));
        }
        Response r = client().performRequest(req);
        assertThat(r.getStatusLine().getStatusCode(), equalTo(200));
    }
}
