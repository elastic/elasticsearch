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
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

/**
 * End-to-end REST coverage for the CRUD API against a cluster with {@code esql-datasource-s3}
 * loaded. Exercises the HTTP layer: status codes, secret masking on the wire, validator error
 * shape. Scenarios needing direct ClusterService access (gateway persistence, race coordination)
 * live in {@code DataSourceCrudIT} in the {@code x-pack:plugin:esql} project's internalClusterTest
 * source set.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class DataSourceCrudRestIT extends ESRestTestCase {

    /** Mirrors {@code org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting.MASK_SENTINEL}. */
    private static final String MASK_SENTINEL = "::es_redacted::";

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

    public void testLifecycle() throws IOException {
        final String name = "lifecycle_ds";

        putDataSource(name, "s3", Map.of("region", "us-east-1", "auth", "anonymous"));

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

    public void testEncryptionResetWipesSecretsButPreservesConfig() throws IOException {
        final String name = "reset_test_ds";
        putDataSource(name, "s3", Map.of("region", "us-east-1", "access_key", "AKIAFAKE", "secret_key", "SECRETVALUE"));

        // Before reset: non-secret present, secrets masked
        Map<String, Object> before = getDataSource(name);
        @SuppressWarnings("unchecked")
        Map<String, Object> beforeSettings = (Map<String, Object>) ((List<Map<String, Object>>) before.get("data_sources")).get(0)
            .get("settings");
        assertThat(beforeSettings.get("region"), equalTo("us-east-1"));
        assertThat(beforeSettings.get("access_key"), equalTo(MASK_SENTINEL));
        assertThat(beforeSettings.get("secret_key"), equalTo(MASK_SENTINEL));

        Request resetReq = new Request("POST", "/_encryption/_reset");
        resetReq.addParameter("accept_data_loss", "true");
        assertThat(client().performRequest(resetReq).getStatusLine().getStatusCode(), equalTo(200));

        // After reset: datasource survives, non-secret config intact, secrets null
        Map<String, Object> after = getDataSource(name);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> afterHits = (List<Map<String, Object>>) after.get("data_sources");
        assertThat(afterHits, hasSize(1));
        assertThat(afterHits.get(0).get("name"), equalTo(name));
        assertThat(afterHits.get(0).get("type"), equalTo("s3"));
        @SuppressWarnings("unchecked")
        Map<String, Object> afterSettings = (Map<String, Object>) afterHits.get(0).get("settings");
        assertThat("non-secret config preserved after reset", afterSettings.get("region"), equalTo("us-east-1"));
        assertTrue("wiped secret key is present", afterSettings.containsKey("access_key"));
        assertNull("wiped secret surfaces as null", afterSettings.get("access_key"));
        assertTrue("wiped secret key is present", afterSettings.containsKey("secret_key"));
        assertNull("wiped secret surfaces as null", afterSettings.get("secret_key"));

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
        putDataSource(parent, "s3", Map.of("region", "us-east-1", "auth", "anonymous"));
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

    public void testListDatasetsWithCoresidentDataStream() throws IOException {
        // Repro for the GET _query/dataset 404 reported 2026-06-30: listing datasets must not blow up just because the
        // cluster also holds an unrelated data stream (the reported one was the Entity Store's entities-updates-default). End to
        // end through the real transport path (action filters + resolution), unlike the resolver unit test.
        final String dataStream = "entities-updates-default";
        Request tmpl = new Request("PUT", "/_index_template/entities-updates-tmpl");
        tmpl.setJsonEntity("{\"index_patterns\":[\"entities-updates-*\"],\"data_stream\":{}}");
        assertThat(client().performRequest(tmpl).getStatusLine().getStatusCode(), equalTo(200));
        Request createDs = new Request("PUT", "/_data_stream/" + dataStream);
        assertThat(client().performRequest(createDs).getStatusLine().getStatusCode(), equalTo(200));

        final String parent = "coresident_parent";
        final String dataset = "cloudtrail_logs";
        putDataSource(parent, "s3", Map.of("region", "us-east-1", "auth", "anonymous"));
        putDataset(dataset, parent, "s3://bucket/cloudtrail/*.json.gz", Map.of());

        // GET /_query/dataset (list all == "*") — this is the exact request from the bug report.
        Response resp = client().performRequest(new Request("GET", "/_query/dataset"));
        assertThat(resp.getStatusLine().getStatusCode(), equalTo(200));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> hits = (List<Map<String, Object>>) entityAsMap(resp).get("datasets");
        assertThat(hits, hasSize(1));
        assertThat(hits.get(0).get("name"), equalTo(dataset));

        deleteDataset(dataset);
        deleteDataSource(parent);
        client().performRequest(new Request("DELETE", "/_data_stream/" + dataStream));
        client().performRequest(new Request("DELETE", "/_index_template/entities-updates-tmpl"));
    }

    public void testGetDatasetByExplicitDataStreamNameReturnsCleanNotFound() throws IOException {
        // GET an explicit name that happens to be a co-resident data stream. The dataset resolver throws
        // IndexNotFoundException (with excluded_ds) before its Type.DATASET filter runs; the GET transport must
        // translate that to a clean dataset-shaped not-found — never leak the raw index_not_found_exception.
        // Mirrors the DELETE behavior. (Before the fix this leaked "excluded_ds"; that was the shape of the reported 404.)
        final String dataStream = "entities-updates-default";
        Request tmpl = new Request("PUT", "/_index_template/entities-updates-tmpl");
        tmpl.setJsonEntity("{\"index_patterns\":[\"entities-updates-*\"],\"data_stream\":{}}");
        assertThat(client().performRequest(tmpl).getStatusLine().getStatusCode(), equalTo(200));
        Request createDs = new Request("PUT", "/_data_stream/" + dataStream);
        assertThat(client().performRequest(createDs).getStatusLine().getStatusCode(), equalTo(200));
        try {
            ResponseException ex = expectThrows(
                ResponseException.class,
                () -> client().performRequest(new Request("GET", "/_query/dataset/" + dataStream))
            );
            assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(404));
            String body = EntityUtils.toString(ex.getResponse().getEntity());
            assertThat(body, containsString("dataset [" + dataStream + "] not found"));
            assertThat("GET must not leak the raw index resolution error", body, not(containsString("excluded_ds")));
            assertThat(body, not(containsString("index_not_found_exception")));
        } finally {
            client().performRequest(new Request("DELETE", "/_data_stream/" + dataStream));
            client().performRequest(new Request("DELETE", "/_index_template/entities-updates-tmpl"));
        }
    }

    public void testGetDatasetMixedValidAndDataStreamNamesNotFound() throws IOException {
        // A comma-separated GET naming a valid dataset and a co-resident data stream: resolution throws on the
        // data stream, so the whole request is a clean not-found that names the offending name (not the valid one,
        // and not the raw index error). Confirms the error reports the specific failed name, mirroring delete.
        final String dataStream = "entities-updates-default";
        Request tmpl = new Request("PUT", "/_index_template/entities-updates-tmpl");
        tmpl.setJsonEntity("{\"index_patterns\":[\"entities-updates-*\"],\"data_stream\":{}}");
        assertThat(client().performRequest(tmpl).getStatusLine().getStatusCode(), equalTo(200));
        Request createDs = new Request("PUT", "/_data_stream/" + dataStream);
        assertThat(client().performRequest(createDs).getStatusLine().getStatusCode(), equalTo(200));
        final String parent = "mixed_parent";
        final String dataset = "valid_ds";
        putDataSource(parent, "s3", Map.of("region", "us-east-1", "auth", "anonymous"));
        putDataset(dataset, parent, "s3://bucket/x/*.parquet", Map.of());
        try {
            ResponseException ex = expectThrows(
                ResponseException.class,
                () -> client().performRequest(new Request("GET", "/_query/dataset/" + dataset + "," + dataStream))
            );
            assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(404));
            String body = EntityUtils.toString(ex.getResponse().getEntity());
            assertThat(body, containsString("dataset [" + dataStream + "] not found"));
            assertThat(body, not(containsString("excluded_ds")));
            assertThat(body, not(containsString("index_not_found_exception")));
        } finally {
            deleteDataset(dataset);
            deleteDataSource(parent);
            client().performRequest(new Request("DELETE", "/_data_stream/" + dataStream));
            client().performRequest(new Request("DELETE", "/_index_template/entities-updates-tmpl"));
        }
    }

    public void testPutDatasetWithMissingParent() throws IOException {
        ResponseException ex = expectThrows(ResponseException.class, () -> putDataset("orphan", "no_such_parent", "s3://x/", Map.of()));
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(404));
        assertThat(EntityUtils.toString(ex.getResponse().getEntity()), containsString("data source [no_such_parent] not found"));
    }

    public void testPutDatasetAcceptsExplicitFormatOnExtensionlessResource() throws IOException {
        // The headline fix: an extensionless resource carries no inferable format, but an explicit
        // `format` dataset setting lets the validator accept that format's settings (here CSV's
        // `delimiter`). Both must round-trip into cluster state so they reach the reader at query time.
        final String parent = "explicit_format_parent";
        final String dataset = "explicit_format_child";
        putDataSource(parent, "s3", Map.of("region", "us-east-1", "auth", "anonymous"));
        putDataset(dataset, parent, "s3://bucket/data", Map.of("format", "csv", "delimiter", "|"));

        Map<String, Object> got = getDataset(dataset);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> hits = (List<Map<String, Object>>) got.get("datasets");
        assertThat(hits, hasSize(1));
        @SuppressWarnings("unchecked")
        Map<String, Object> settings = (Map<String, Object>) hits.get(0).get("settings");
        assertThat("explicit format round-trips in its canonical lowercase form", settings.get("format"), equalTo("csv"));
        assertThat("the csv-specific delimiter round-trips", settings.get("delimiter"), equalTo("|"));

        deleteDataset(dataset);
        deleteDataSource(parent);
    }

    public void testPutDatasetRejectsUnknownFormat() throws IOException {
        final String parent = "unknown_format_parent";
        putDataSource(parent, "s3", Map.of("region", "us-east-1", "auth", "anonymous"));
        ResponseException ex = expectThrows(
            ResponseException.class,
            () -> putDataset("unknown_format_child", parent, "s3://bucket/data", Map.of("format", "bogus"))
        );
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(EntityUtils.toString(ex.getResponse().getEntity()), containsString("unknown format [bogus]"));
        deleteDataSource(parent);
    }

    public void testPutDatasetRejectsFormatSpecificSettingWithoutResolvableFormat() throws IOException {
        // Extensionless resource + a format-specific setting but no `format`: the validator cannot tell
        // which format the setting belongs to, so it fails with the targeted "cannot determine format" hint.
        final String parent = "no_format_parent";
        putDataSource(parent, "s3", Map.of("region", "us-east-1", "auth", "anonymous"));
        ResponseException ex = expectThrows(
            ResponseException.class,
            () -> putDataset("no_format_child", parent, "s3://bucket/data", Map.of("delimiter", "|"))
        );
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(EntityUtils.toString(ex.getResponse().getEntity()), containsString("cannot determine format"));
        deleteDataSource(parent);
    }

    public void testPutDatasetRejectsUnknownTopLevelField() throws IOException {
        final String parent = "reject_field_parent";
        putDataSource(parent, "s3", Map.of("region", "us-east-1", "auth", "anonymous"));
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
        putDataSource(parent, "s3", Map.of("region", "us-east-1", "auth", "anonymous"));
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
