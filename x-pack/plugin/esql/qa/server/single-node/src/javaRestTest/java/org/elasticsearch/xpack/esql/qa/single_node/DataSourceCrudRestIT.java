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
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * End-to-end REST coverage for the CRUD API against a cluster with {@code esql-datasource-s3}
 * loaded. Exercises the HTTP layer: status codes, secret masking on the wire, validator error
 * shape. Scenarios needing direct ClusterService access (gateway persistence, race coordination)
 * live in {@code DataSourceCrudIT} in the {@code x-pack:plugin:esql} project's internalClusterTest
 * source set.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class DataSourceCrudRestIT extends ESRestTestCase {

    /** Mirrors {@code org.elasticsearch.cluster.metadata.DataSourceSetting.MASK_SENTINEL}. */
    private static final String MASK_SENTINEL = "::es_redacted::";

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testLifecycle() throws IOException {
        final String name = "lifecycle_ds";

        putDataSource(name, "s3", Map.of("region", "us-east-1"));

        Map<String, Object> got = getDataSource(name);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> hits = (List<Map<String, Object>>) got.get("data_sources");
        assertThat(hits, hasSize(1));
        assertThat(hits.get(0).get("name"), equalTo(name));
        assertThat(hits.get(0).get("type"), equalTo("s3"));

        deleteDataSource(name);
        ResponseException missing = expectThrows(ResponseException.class, () -> getDataSource(name));
        assertThat(missing.getResponse().getStatusLine().getStatusCode(), equalTo(404));
    }

    public void testSecretMaskedInResponse() throws IOException {
        final String name = "secret_rt";
        putDataSource(name, "s3", Map.of("region", "us-west-2", "access_key", "AKIAFAKE", "secret_key", "SECRETVALUE"));

        Map<String, Object> got = getDataSource(name);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> hits = (List<Map<String, Object>>) got.get("data_sources");
        assertThat(hits, hasSize(1));
        @SuppressWarnings("unchecked")
        Map<String, Object> settings = (Map<String, Object>) hits.get(0).get("settings");
        assertThat("region round-trips plain", settings.get("region"), equalTo("us-west-2"));
        assertThat("access_key surfaces as the mask sentinel", settings.get("access_key"), equalTo(MASK_SENTINEL));
        assertThat("secret_key surfaces as the mask sentinel", settings.get("secret_key"), equalTo(MASK_SENTINEL));

        deleteDataSource(name);
    }

    public void testPutDataSourceRejectsUnknownTopLevelField() throws IOException {
        Request req = new Request("PUT", "/_query/data_source/bogus_field_ds");
        try (XContentBuilder b = jsonBuilder()) {
            b.startObject().field("type", "s3").field("not_a_real_field", "x").endObject();
            req.setJsonEntity(Strings.toString(b));
        }
        ResponseException ex = expectThrows(ResponseException.class, () -> client().performRequest(req));
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(400));
    }

    public void testDatasetLifecycle() throws IOException {
        final String parent = "ds_lifecycle_parent";
        final String dataset = "ds_lifecycle_child";
        putDataSource(parent, "s3", Map.of("region", "us-east-1"));
        putDataset(dataset, parent, "s3://bucket/x/*.parquet", Map.of());

        Map<String, Object> got = getDataset(dataset);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> hits = (List<Map<String, Object>>) got.get("datasets");
        assertThat(hits, hasSize(1));
        assertThat(hits.get(0).get("name"), equalTo(dataset));
        assertThat(hits.get(0).get("data_source"), equalTo(parent));
        assertThat(hits.get(0).get("resource"), equalTo("s3://bucket/x/*.parquet"));

        deleteDataset(dataset);
        ResponseException missing = expectThrows(ResponseException.class, () -> getDataset(dataset));
        assertThat(missing.getResponse().getStatusLine().getStatusCode(), equalTo(404));

        deleteDataSource(parent);
    }

    public void testPutDatasetWithMissingParent() throws IOException {
        ResponseException ex = expectThrows(ResponseException.class, () -> putDataset("orphan", "no_such_parent", "s3://x/", Map.of()));
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(404));
        assertThat(EntityUtils.toString(ex.getResponse().getEntity()), containsString("data source [no_such_parent] not found"));
    }

    public void testPutDatasetRejectsUnknownTopLevelField() throws IOException {
        final String parent = "reject_field_parent";
        putDataSource(parent, "s3", Map.of("region", "us-east-1"));
        Request req = new Request("PUT", "/_query/dataset/reject_field_ds");
        try (XContentBuilder b = jsonBuilder()) {
            b.startObject().field("data_source", parent).field("resource", "s3://x/").field("not_a_real_field", "x").endObject();
            req.setJsonEntity(Strings.toString(b));
        }
        ResponseException ex = expectThrows(ResponseException.class, () -> client().performRequest(req));
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        deleteDataSource(parent);
    }

    public void testDeleteDataSourceWithDependentsReturns409() throws IOException {
        final String parent = "delete_blocked_parent";
        final String dataset = "delete_blocked_child";
        putDataSource(parent, "s3", Map.of("region", "us-east-1"));
        putDataset(dataset, parent, "s3://x/", Map.of());

        ResponseException ex = expectThrows(
            ResponseException.class,
            () -> client().performRequest(new Request("DELETE", "/_query/data_source/" + parent))
        );
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(409));
        assertThat(EntityUtils.toString(ex.getResponse().getEntity()), containsString("referenced by datasets"));

        deleteDataset(dataset);
        deleteDataSource(parent);
    }

    public void testValidatorRejectsUnknownType() throws IOException {
        ResponseException ex = expectThrows(
            ResponseException.class,
            () -> putDataSource("bad_type", "definitely_not_a_real_type", Map.of())
        );
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(EntityUtils.toString(ex.getResponse().getEntity()), containsString("unknown data source type"));
    }

    public void testValidatorRejectsUnknownSetting() throws IOException {
        ResponseException ex = expectThrows(
            ResponseException.class,
            () -> putDataSource("bad_setting", "s3", Map.of("region", "us-east-1", "not_a_real_setting", "x"))
        );
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(400));
    }

    public void testValidatorRejectsInvalidName() {
        ResponseException ex = expectThrows(ResponseException.class, () -> putDataSource("BadName", "s3", Map.of()));
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(400));
    }

    private static Map<String, Object> getDataSource(String name) throws IOException {
        Response resp = client().performRequest(new Request("GET", "/_query/data_source/" + name));
        return entityAsMap(resp);
    }

    private static void putDataSource(String name, String type, Map<String, Object> settings) throws IOException {
        Request req = new Request("PUT", "/_query/data_source/" + name);
        try (XContentBuilder b = jsonBuilder()) {
            b.startObject().field("type", type);
            if (settings.isEmpty() == false) {
                b.field("settings", settings);
            }
            b.endObject();
            req.setJsonEntity(Strings.toString(b));
        }
        Response r = client().performRequest(req);
        assertThat(r.getStatusLine().getStatusCode(), equalTo(200));
    }

    private static void deleteDataSource(String name) throws IOException {
        Response r = client().performRequest(new Request("DELETE", "/_query/data_source/" + name));
        assertThat(r.getStatusLine().getStatusCode(), equalTo(200));
    }

    private static Map<String, Object> getDataset(String name) throws IOException {
        Response resp = client().performRequest(new Request("GET", "/_query/dataset/" + name));
        return entityAsMap(resp);
    }

    private static void putDataset(String name, String dataSource, String resource, Map<String, Object> settings) throws IOException {
        Request req = new Request("PUT", "/_query/dataset/" + name);
        try (XContentBuilder b = jsonBuilder()) {
            b.startObject().field("data_source", dataSource).field("resource", resource);
            if (settings.isEmpty() == false) {
                b.field("settings", settings);
            }
            b.endObject();
            req.setJsonEntity(Strings.toString(b));
        }
        Response r = client().performRequest(req);
        assertThat(r.getStatusLine().getStatusCode(), equalTo(200));
    }

    private static void deleteDataset(String name) throws IOException {
        Response r = client().performRequest(new Request("DELETE", "/_query/dataset/" + name));
        assertThat(r.getStatusLine().getStatusCode(), equalTo(200));
    }
}
