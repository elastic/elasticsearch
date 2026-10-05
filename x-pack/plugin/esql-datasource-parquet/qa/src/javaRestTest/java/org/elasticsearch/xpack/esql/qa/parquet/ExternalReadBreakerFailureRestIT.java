/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.parquet;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xcontent.ObjectPath;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.datasources.AbstractFromDatasetSubqueryRestTestCase;
import org.elasticsearch.xpack.esql.datasources.BackendFixture;
import org.elasticsearch.xpack.esql.datasources.S3BackendFixture;
import org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.DataSourcesS3HttpFixture;
import org.elasticsearch.xpack.esql.qa.parquet.EmployeesParquetGenerator.EmployeeRow;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.WAREHOUSE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

/**
 * A query over an external Parquet object that trips the parent circuit breaker while reading must come back to the
 * client as a {@code 429 circuit_breaking_exception} whose body is small enough to render safely, even with
 * {@code error_trace=true}. See elastic/esql-planning#2135, where a failure graph that looped through suppressed
 * exceptions made the error body grow until the node ran out of heap.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class ExternalReadBreakerFailureRestIT extends AbstractFromDatasetSubqueryRestTestCase {

    private static final String DATA_SOURCE = "breaker_failure_s3_ds";
    private static final String DATASET = "breaker_failure_employees";
    private static final String BLOB_KEY = WAREHOUSE + "/standalone/breaker_failure_employees.parquet";
    private static final String PARENT_LIMIT_SETTING = "indices.breaker.total.limit";

    /** Well above the reader's 4 MiB sliding window, so reading the data needs a full window. */
    private static final int ROW_COUNT = 150_000;
    /** Headroom over the parent breaker's idle usage: room for the REST requests, not for the reader's window. */
    private static final long PARENT_HEADROOM_BYTES = 2 * 1024 * 1024;
    private static final int MAX_ERROR_BODY_BYTES = 64 * 1024;

    public static DataSourcesS3HttpFixture s3Fixture = new DataSourcesS3HttpFixture();
    public static ElasticsearchCluster cluster = Clusters.reservationParentBreakerTestClusterWithEncryption(() -> s3Fixture.getAddress());

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(s3Fixture).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @After
    public void resetParentLimit() throws Exception {
        // the reset request itself is charged to the lowered parent breaker, so it may trip while the failed query's
        // reservations are still being released
        assertBusy(() -> {
            try {
                setParentLimit(null);
            } catch (ResponseException e) {
                throw new AssertionError(e);
            }
        });
    }

    @AfterClass
    public static void cleanupRegistry() throws IOException {
        deleteIgnoringMissing("/_query/dataset/" + DATASET);
        deleteIgnoringMissing("/_query/data_source/" + DATA_SOURCE);
    }

    public void testBreakerTripDuringExternalReadReturnsASmall429() throws Exception {
        BackendFixture s3Backend = new S3BackendFixture(s3Fixture);
        s3Backend.uploadBlob(BLOB_KEY, largeEmployeesParquetBytes());
        putDataSource(DATA_SOURCE, s3Backend.dataSourceType(), s3Backend.dataSourceSettings());
        putDataset(DATASET, DATA_SOURCE, s3Backend.resourceUri(BLOB_KEY), Map.of());

        // The parent limit is a percentage of the heap (an absolute value logs a critical deprecation); derive the
        // percentage from this node's heap so the limit lands just above idle usage whatever heap the node has.
        Map<String, Object> node = singleNodeStats();
        long heapMax = ObjectPath.<Number>eval("jvm.mem.heap_max_in_bytes", node).longValue();
        long parentUsed = ObjectPath.<Number>eval("breakers.parent.estimated_size_in_bytes", node).longValue();
        double percent = (parentUsed + PARENT_HEADROOM_BYTES) * 100.0 / heapMax;
        setParentLimit(String.format(Locale.ROOT, "%.4f%%", percent));

        Request query = new Request("POST", "/_query");
        query.addParameter("error_trace", "true");
        query.addParameter("allow_partial_results", "false");
        try (XContentBuilder b = jsonBuilder()) {
            b.startObject().field("query", "FROM " + DATASET + " | WHERE first_name LIKE \"*q*\" | STATS c = COUNT(*)").endObject();
            query.setJsonEntity(Strings.toString(b));
        }
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(query));
        Response response = e.getResponse();
        assertThat(response.getStatusLine().getStatusCode(), equalTo(429));
        byte[] body = EntityUtils.toByteArray(response.getEntity());
        assertThat(body.length, lessThan(MAX_ERROR_BODY_BYTES));
        Map<String, Object> parsed = XContentHelper.convertToMap(XContentType.JSON.xContent(), body, 0, body.length, false);
        assertThat(ObjectPath.eval("error.type", parsed), equalTo("circuit_breaking_exception"));
        assertThat(ObjectPath.eval("error.reason", parsed), containsString("parquet"));
    }

    private static Map<String, Object> singleNodeStats() throws IOException {
        Map<String, Object> stats = entityAsMap(client().performRequest(new Request("GET", "/_nodes/stats/jvm,breaker")));
        @SuppressWarnings("unchecked")
        Map<String, Object> nodes = (Map<String, Object>) stats.get("nodes");
        assertThat(nodes.size(), equalTo(1));
        @SuppressWarnings("unchecked")
        Map<String, Object> node = (Map<String, Object>) nodes.values().iterator().next();
        return node;
    }

    /**
     * Sets, or with {@code null} resets, the parent breaker limit. A limit below the recommended minimum makes the
     * update respond with a deprecation warning, which is expected here.
     */
    private static void setParentLimit(String limit) throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        try (XContentBuilder b = jsonBuilder()) {
            b.startObject().startObject("persistent").field(PARENT_LIMIT_SETTING, limit).endObject().endObject();
            request.setJsonEntity(Strings.toString(b));
        }
        request.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(warnings -> false));
        client().performRequest(request);
    }

    private static byte[] largeEmployeesParquetBytes() throws IOException {
        EmployeeRow[] rows = new EmployeeRow[ROW_COUNT];
        for (int i = 0; i < ROW_COUNT; i++) {
            rows[i] = new EmployeeRow(i, randomAlphaOfLength(16), randomAlphaOfLength(16), randomIntBetween(10_000, 100_000));
        }
        return EmployeesParquetGenerator.employeesParquetBytes(rows);
    }
}
