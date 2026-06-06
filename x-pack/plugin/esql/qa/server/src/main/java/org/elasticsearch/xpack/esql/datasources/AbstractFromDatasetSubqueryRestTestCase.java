/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.BeforeClass;

import java.io.IOException;
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
 *   <li>Static REST helpers for {@code PUT /_query/data_source/...}, {@code PUT /_query/dataset/...},
 *       {@code POST /_query}, and idempotent dataset/data-source cleanup.</li>
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
     * pointing at the in-process fixture.
     */
    protected static void putDataSource(String name, String type, Map<String, Object> settings) throws IOException {
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

    /**
     * {@code PUT /_query/dataset/<name>} bound to the supplied {@code data_source} + {@code resource}
     * URI. {@code settings} may carry {@code format} or any format-specific keys the validator
     * accepts (e.g. {@code optimized_reader} for parquet, {@code delimiter} for csv).
     */
    protected static void putDataset(String name, String dataSource, String resource, Map<String, Object> settings) throws IOException {
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

    /** {@code POST /_query} and return the response as a map. Asserts a 200 status. */
    protected static Map<String, Object> runQuery(String esql) throws IOException {
        Request req = new Request("POST", "/_query");
        try (XContentBuilder b = jsonBuilder()) {
            b.startObject().field("query", esql).endObject();
            req.setJsonEntity(Strings.toString(b));
        }
        Response r = client().performRequest(req);
        assertThat(r.getStatusLine().getStatusCode(), equalTo(200));
        return entityAsMap(r);
    }

    /**
     * {@code DELETE <path>} that swallows 404s so {@code @AfterClass} sweeps don't fail when a
     * registration step was skipped (e.g. the test method short-circuited before the dataset was
     * created).
     */
    protected static void deleteIgnoringMissing(String path) throws IOException {
        try {
            client().performRequest(new Request("DELETE", path));
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() != 404) {
                throw e;
            }
        }
    }

    /**
     * Asserts a single output row of the {@code KEEP emp_no, first_name} projection every
     * concrete suite uses. Centralized so backends/formats stay row-shape consistent.
     */
    protected static void assertEmployeeRow(List<Object> row, int expectedEmpNo, String expectedFirstName) {
        assertThat(((Number) row.get(0)).intValue(), equalTo(expectedEmpNo));
        assertThat(row.get(1).toString(), equalTo(expectedFirstName));
    }
}
