/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

/**
 * Shared base for REST-driven {@code FROM (FROM <dataset> | ...)} integration tests that exercise
 * the production {@code esql-datasource-*} plugins end-to-end against the in-process S3, GCS, and
 * Azure HTTP fixtures.
 *
 * <p>The base owns only the parts that don't vary between concrete suites:
 * <ul>
 *   <li>The {@link #requireSnapshotBuild()} {@code @BeforeClass} gate matching
 *       {@code DataSourceCrudRestIT} (datasets are snapshot-only today).</li>
 *   <li>Static REST helpers for {@code PUT /_query/data_source/...}, {@code PUT /_query/dataset/...}
 *       (delegating to {@link DatasetRegistry}), {@code POST /_query}, and tolerant dataset/data-source
 *       cleanup.</li>
 *   <li>A canonical row asserter for the {@code {emp_no, first_name}} projection every concrete
 *       suite uses.</li>
 * </ul>
 *
 * <p>Concrete subclasses still declare their own {@code @ClassRule} fixtures, cluster, and
 * {@code @ParametersFactory} list — those have to live in the subclass because the rule chain
 * and cluster wiring is per-suite (single-format suites use one fixture per backend; the
 * cross-format suite uses two). The base just provides the verbs.
 */
public abstract class AbstractFromDatasetSubqueryRestTestCase extends ESRestTestCase {

    /**
     * Dataset CRUD and EXTERNAL data sources are gated to snapshot builds today (same gate as
     * {@code DataSourceCrudRestIT}). Skip cleanly on release builds rather than fail in
     * {@code @BeforeClass} of the concrete suite.
     */
    @BeforeClass
    public static void requireSnapshotBuild() {
        assumeTrue("datasources not available in release builds yet", Build.current().isSnapshot());
    }

    /**
     * {@code PUT /_query/data_source/<name>} with {@code type} and (optional) {@code settings}.
     * Used by every backend wrapper to register an {@code s3}/{@code gcs}/{@code azure} data source
     * pointing at the in-process fixture. Delegates to {@link DatasetRegistry} so the request-building
     * lives in one place; this base manages its own lifecycle, so the uncached verb is used.
     */
    protected static void putDataSource(String name, String type, Map<String, Object> settings) throws IOException {
        DatasetRegistry.putDataSource(client(), name, type, settings);
    }

    /**
     * {@code PUT /_query/dataset/<name>} bound to the supplied {@code data_source} + {@code resource}
     * URI. {@code settings} may carry {@code format} or any format-specific keys the validator
     * accepts (e.g. {@code optimized_reader} for parquet, {@code delimiter} for csv). Delegates to
     * {@link DatasetRegistry}.
     */
    protected static void putDataset(String name, String dataSource, String resource, Map<String, Object> settings) throws IOException {
        DatasetRegistry.putDataset(client(), name, dataSource, resource, settings);
    }

    /** The benign warning ES|QL emits when a query omits an explicit {@code LIMIT}. */
    private static final String DEFAULT_LIMIT_WARNING = "No limit defined, adding default limit of [1000]";

    /**
     * {@code POST /_query} and return the response as a map. Asserts a 200 status.
     *
     * <p>The {@code FROM (FROM <dataset> | ...)} queries here intentionally omit an explicit
     * {@code LIMIT}, so ES|QL appends a default limit and emits {@link #DEFAULT_LIMIT_WARNING}. That
     * single warning is expected and tolerated; any other warning still fails the request.
     */
    protected static Map<String, Object> runQuery(String esql) throws IOException {
        Request req = new Request("POST", "/_query");
        try (XContentBuilder b = jsonBuilder()) {
            b.startObject().field("query", esql).endObject();
            req.setJsonEntity(Strings.toString(b));
        }
        req.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(warnings -> {
            List<String> unexpected = new ArrayList<>(warnings);
            unexpected.remove(DEFAULT_LIMIT_WARNING);
            return unexpected.isEmpty() == false;
        }));
        Response r = client().performRequest(req);
        assertThat(r.getStatusLine().getStatusCode(), equalTo(200));
        return entityAsMap(r);
    }

    /**
     * {@code DELETE <path>} that swallows 404s so {@code @AfterClass} sweeps don't fail when a
     * registration step was skipped (e.g. the test method short-circuited before the dataset was
     * created). Delegates to {@link DatasetRegistry#deleteIgnoringMissing}.
     */
    protected static void deleteIgnoringMissing(String path) throws IOException {
        DatasetRegistry.deleteIgnoringMissing(client(), path);
    }

    /**
     * Asserts a single output row of the {@code KEEP emp_no, first_name} projection every
     * concrete suite uses. Centralized so backends/formats stay row-shape consistent.
     */
    protected static void assertEmployeeRow(List<Object> row, int expectedEmpNo, String expectedFirstName) {
        assertThat(((Number) row.get(0)).intValue(), equalTo(expectedEmpNo));
        assertThat(row.get(1).toString(), equalTo(expectedFirstName));
    }

    /**
     * Creates a {@code time_series}-mode index ({@code index.mode=time_series}) with a {@code name} keyword
     * dimension (the IN join key), the required {@code @timestamp} field and a {@code value} gauge metric, then
     * bulk-indexes one row per supplied {@code name} and refreshes. Read via {@code FROM} the index surfaces as a
     * regular relation, so {@code name} can be an IN join key against an external dataset's keyword column. Shared
     * across the concrete suites so the time-series shape stays consistent across formats/backends.
     */
    protected static void createTimeSeriesIndex(String index, String... names) throws IOException {
        Request create = new Request("PUT", "/" + index);
        try (XContentBuilder b = jsonBuilder()) {
            b.startObject();
            b.startObject("settings")
                .field("index.mode", "time_series")
                .array("index.routing_path", "name")
                .field("index.number_of_shards", 1)
                .field("index.number_of_replicas", 0)
                .endObject();
            b.startObject("mappings").startObject("properties");
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject("name").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject("value").field("type", "double").field("time_series_metric", "gauge").endObject();
            b.endObject().endObject();
            b.endObject();
            create.setJsonEntity(Strings.toString(b));
        }
        assertThat(client().performRequest(create).getStatusLine().getStatusCode(), equalTo(200));

        StringBuilder bulk = new StringBuilder();
        double value = 0.1;
        for (String name : names) {
            bulk.append("{\"create\":{}}\n");
            bulk.append("{\"@timestamp\":\"2025-01-01T00:00:00Z\",\"name\":\"")
                .append(name)
                .append("\",\"value\":")
                .append(value)
                .append("}\n");
            value += 0.1;
        }
        Request bulkReq = new Request("POST", "/" + index + "/_bulk");
        bulkReq.addParameter("refresh", "true");
        bulkReq.setJsonEntity(bulk.toString());
        assertThat(client().performRequest(bulkReq).getStatusLine().getStatusCode(), equalTo(200));
    }

    /**
     * Creates a plain (non-time-series) index with the {@code emp_no:integer, first_name:keyword} shape the
     * multi-backend union assertions use, then bulk-indexes the supplied {@code emp_no -> first_name} rows and
     * refreshes. Lets the union test add a regular-index subquery branch alongside the blob-backed dataset
     * branches; the matching {@code emp_no}/{@code first_name} columns union cleanly with those branches.
     */
    protected static void createEmployeeIndex(String index, Map<Integer, String> employees) throws IOException {
        Request create = new Request("PUT", "/" + index);
        try (XContentBuilder b = jsonBuilder()) {
            b.startObject();
            b.startObject("settings").field("index.number_of_shards", 1).field("index.number_of_replicas", 0).endObject();
            b.startObject("mappings").startObject("properties");
            b.startObject("emp_no").field("type", "integer").endObject();
            b.startObject("first_name").field("type", "keyword").endObject();
            b.endObject().endObject();
            b.endObject();
            create.setJsonEntity(Strings.toString(b));
        }
        assertThat(client().performRequest(create).getStatusLine().getStatusCode(), equalTo(200));

        StringBuilder bulk = new StringBuilder();
        for (Map.Entry<Integer, String> e : employees.entrySet()) {
            bulk.append("{\"index\":{}}\n");
            bulk.append("{\"emp_no\":").append(e.getKey()).append(",\"first_name\":\"").append(e.getValue()).append("\"}\n");
        }
        Request bulkReq = new Request("POST", "/" + index + "/_bulk");
        bulkReq.addParameter("refresh", "true");
        bulkReq.setJsonEntity(bulk.toString());
        assertThat(client().performRequest(bulkReq).getStatusLine().getStatusCode(), equalTo(200));
    }

    /**
     * Creates a {@code time_series}-mode index that carries the same {@code emp_no:integer, first_name:keyword}
     * pair the union assertions use: {@code first_name} is the time-series dimension (the union's keyword column)
     * and {@code emp_no} is a regular integer field, alongside the required {@code @timestamp} and a {@code value}
     * gauge metric. Read via {@code TS <index>} the relation surfaces in time-series mode, so the union test can
     * add a genuine {@code TS} subquery branch whose {@code emp_no}/{@code first_name} columns still union cleanly
     * (matching integer/keyword types) with the dataset and regular-index branches.
     */
    protected static void createTimeSeriesEmployeeIndex(String index, Map<Integer, String> employees) throws IOException {
        Request create = new Request("PUT", "/" + index);
        try (XContentBuilder b = jsonBuilder()) {
            b.startObject();
            b.startObject("settings")
                .field("index.mode", "time_series")
                .array("index.routing_path", "first_name")
                .field("index.number_of_shards", 1)
                .field("index.number_of_replicas", 0)
                .endObject();
            b.startObject("mappings").startObject("properties");
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject("first_name").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject("emp_no").field("type", "integer").endObject();
            b.startObject("value").field("type", "double").field("time_series_metric", "gauge").endObject();
            b.endObject().endObject();
            b.endObject();
            create.setJsonEntity(Strings.toString(b));
        }
        assertThat(client().performRequest(create).getStatusLine().getStatusCode(), equalTo(200));

        StringBuilder bulk = new StringBuilder();
        double value = 0.1;
        for (Map.Entry<Integer, String> e : employees.entrySet()) {
            bulk.append("{\"create\":{}}\n");
            bulk.append("{\"@timestamp\":\"2025-01-01T00:00:00Z\",\"first_name\":\"")
                .append(e.getValue())
                .append("\",\"emp_no\":")
                .append(e.getKey())
                .append(",\"value\":")
                .append(value)
                .append("}\n");
            value += 0.1;
        }
        Request bulkReq = new Request("POST", "/" + index + "/_bulk");
        bulkReq.addParameter("refresh", "true");
        bulkReq.setJsonEntity(bulk.toString());
        assertThat(client().performRequest(bulkReq).getStatusLine().getStatusCode(), equalTo(200));
    }
}
