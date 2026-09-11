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
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * End-to-end REST coverage for the {@code wildcard_datasets} query setting, against the scenario reported on a 9.5.2
 * cluster in elastic/elasticsearch#158472: enough registered datasets make every {@code FROM *} fail, because a
 * wildcard sweeps them all in and each becomes its own plan branch.
 *
 * <p>Eight datasets plus one matching index is nine branches, one past the per-{@code FROM} cap, so the wildcard fails
 * outright when it may discover datasets. With the setting at its default the same wildcard means index-likes only and
 * the query returns the index rows. Run against a tree without the setting, {@link #testWildcardResolvesToIndicesOnly}
 * fails with that branch-limit error.
 *
 * <p>This is the layer that can register a data source, so it sits beside {@link DataSourceCrudRestIT} rather than in
 * the {@code x-pack:plugin:esql} unit suites.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class WildcardDatasetSettingRestIT extends ESRestTestCase {

    /** One past {@code MergePlan.MAX_BRANCHES} once the matching index contributes its own branch. */
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

    public void testWildcardResolvesToIndicesOnly() throws IOException {
        // The default: the wildcard means index-likes, so the registered datasets neither contribute branches nor are
        // read. The query returns what FROM logs-000001 returns. KEEP pins the projection because the dynamic mapping
        // gives `message` a `.keyword` sub-field, and the explicit LIMIT keeps the default-limit warning off the wire.
        Map<String, Object> response = query("FROM * | KEEP message | LIMIT 10");

        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) response.get("values");
        assertThat(values, hasSize(1));
        assertThat(values.get(0), equalTo(List.of("hello")));
    }

    public void testWildcardDiscoversDatasetsWhenSettingIsOn() throws IOException {
        // Opting the query into wildcard discovery restores the reported behavior: eight datasets plus the index is
        // nine branches, one past the cap. The point is that the setting decides it, not that the cap is right.
        ResponseException ex = expectThrows(
            ResponseException.class,
            () -> query("SET wildcard_datasets = true; FROM * | KEEP message | LIMIT 10")
        );
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        String body = EntityUtils.toString(ex.getResponse().getEntity());
        assertThat(body, containsString("exceeding the current limit of"));
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
