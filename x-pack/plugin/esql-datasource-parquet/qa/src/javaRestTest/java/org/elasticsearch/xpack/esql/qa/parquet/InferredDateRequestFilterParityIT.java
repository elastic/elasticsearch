/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.parquet;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.AzureReactorThreadFilter;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.AssertWarnings;
import org.elasticsearch.xpack.esql.datasources.DatasetRegistry;
import org.elasticsearch.xpack.esql.datasources.FixtureUtils;
import org.elasticsearch.xpack.esql.qa.parquet.EmployeesParquetGenerator.EventRow;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.requestObjectBuilder;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.runEsqlSync;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.lessThan;

/**
 * End-to-end proof, over the real {@code _query} HTTP API, that the out-of-band request {@code filter} a Kibana
 * dashboard sends selects the same rows on an external <b>dataset</b> as on an index holding the same data — including
 * a {@code range} on a <b>date</b> field, which is the shape a dashboard time picker produces.
 *
 * <p>The date field is <b>inferred</b>, not declared: the fixture is a Parquet file whose {@code event_time} column is
 * a {@code TIMESTAMP(MILLIS)}, which the reader surfaces as ES|QL {@code date} with no declared mapping. So this
 * exercises the currently-working inference path rather than the declared-schema path.
 *
 * <p>The request body is exactly what Kibana emits: {@code buildEsQuery} compiles the time picker + a filter pill into
 * one {@code bool} placed in the {@code filter} parameter of the {@code _query} request (a {@code range} on the time
 * field plus a {@code match_phrase} pill). We send that same body to {@code FROM <dataset>} and {@code FROM <index>}
 * and assert the selected id set is identical, and non-trivial (the filter really carved a subset).
 */
@ThreadLeakFilters(filters = { TestClustersThreadFilter.class, AzureReactorThreadFilter.class })
public class InferredDateRequestFilterParityIT extends ESRestTestCase {

    private static final Logger logger = LogManager.getLogger(InferredDateRequestFilterParityIT.class);

    private static final int ROWS = 30;
    private static final Instant BASE = Instant.parse("2024-01-01T00:00:00Z");

    private static final String LOCAL_DATA_SOURCE = "local_parquet_ds";
    private static final String DATASET = "events_dataset";
    private static final String RENAMED_DATASET = "events_renamed_ts";
    private static final String INDEX = "events_index";
    private static final String FIXTURE = "events.parquet";

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.httpOnlyTestCluster();

    private static Path localFixturesPath;
    private static boolean datasetRegistered = false;

    /** Datasets and {@code local} data sources are gated to snapshot builds today (same gate as the other datasource ITs). */
    @BeforeClass
    public static void requireSnapshotBuild() {
        assumeTrue("datasources not available in release builds yet", Build.current().isSnapshot());
    }

    @BeforeClass
    public static void writeFixture() throws Exception {
        localFixturesPath = FixtureUtils.resolveLocalFixturesPath(logger, InferredDateRequestFilterParityIT.class);
        assumeTrue("LOCAL fixtures unavailable (packaged in a JAR)", localFixturesPath != null);
        // Generate the Parquet fixture at build time under the node's allowed local-read root, so the dataset reads a
        // real file:// resource with an inferred date column and no network dependency.
        Files.write(localFixturesPath.resolve(FIXTURE), EmployeesParquetGenerator.eventLogParquetBytes(rows()));
    }

    @AfterClass
    public static void cleanupDatasets() throws Exception {
        try {
            // Delete the raw-PUT renamed dataset first — DatasetRegistry doesn't track it, and the data source it
            // references can't be dropped by DatasetRegistry.cleanup() while a dataset still points at it.
            DatasetRegistry.deleteIgnoringMissing(client(), "/_query/dataset/" + RENAMED_DATASET);
            DatasetRegistry.cleanup(client());
        } finally {
            datasetRegistered = false;
            DatasetRegistry.clearCaches();
        }
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    /** id i: category cycles login/logout/error; event_time is BASE + i days. */
    private static EventRow[] rows() {
        EventRow[] out = new EventRow[ROWS];
        for (int i = 0; i < ROWS; i++) {
            out[i] = new EventRow(i, category(i), BASE.plus(i, ChronoUnit.DAYS).toEpochMilli());
        }
        return out;
    }

    private static String category(int i) {
        return switch (i % 3) {
            case 0 -> "login";
            case 1 -> "logout";
            default -> "error";
        };
    }

    @Before
    public void loadBothSources() throws Exception {
        // The dataset: a file:// Parquet source, schema inferred from the .parquet extension (event_time -> date).
        String uri = localFixturesPath.resolve(FIXTURE).toUri().toString();
        DatasetRegistry.ensureDataSource(client(), LOCAL_DATA_SOURCE, "local", Map.of());
        DatasetRegistry.ensureDataset(client(), DATASET, LOCAL_DATA_SOURCE, uri, null);

        // A second dataset over the same file with a DYNAMIC (inferred) base plus one declared column that RENAMES the
        // source event_time to @timestamp via the read-path `path` — "a time axis is just a column named @timestamp".
        // This is what surfaces an @timestamp column that Kibana's `FROM <source> | LIMIT 0` probe resolves without
        // field_caps. DatasetRegistry only sends settings, so the mapping goes through a raw PUT.
        if (datasetRegistered == false) {
            Request putRenamed = new Request("PUT", "/_query/dataset/" + RENAMED_DATASET);
            putRenamed.setJsonEntity(String.format(Locale.ROOT, """
                {
                  "data_source": "%s",
                  "resource": "%s",
                  "mappings": { "dynamic": "true", "properties": { "@timestamp": { "type": "date", "path": "event_time" } } }
                }
                """, LOCAL_DATA_SOURCE, uri));
            assertOK(client().performRequest(putRenamed));
            datasetRegistered = true;
        }

        // The mirror index: identical rows, event_time mapped as a date (epoch_millis), category as keyword.
        if (indexExists(INDEX) == false) {
            createIndex(INDEX, Settings.EMPTY, """
                "properties": {
                  "id": { "type": "integer" },
                  "category": { "type": "keyword" },
                  "event_time": { "type": "date", "format": "epoch_millis" }
                }
                """);
            StringBuilder bulk = new StringBuilder();
            for (EventRow r : rows()) {
                bulk.append("{\"index\":{}}\n");
                bulk.append(
                    String.format(
                        Locale.ROOT,
                        "{\"id\":%d,\"category\":\"%s\",\"event_time\":%d}\n",
                        r.id(),
                        r.category(),
                        r.eventTimeMillis()
                    )
                );
            }
            Request bulkRequest = new Request("POST", "/" + INDEX + "/_bulk?refresh=true");
            bulkRequest.setJsonEntity(bulk.toString());
            assertOK(client().performRequest(bulkRequest));
        }
    }

    /** A time-range-only filter (the dashboard time picker): a range on the inferred date must select the same rows. */
    public void testTimeRangeOnInferredDate() throws Exception {
        String from = iso(5);
        String to = iso(20);
        assertSameIds(bool -> {
            bool.startArray("filter").startObject().startObject("range").startObject("event_time");
            bool.field("gte", from).field("lte", to).field("format", "strict_date_optional_time");
            bool.endObject().endObject().endObject().endArray();
        });
    }

    /** The realistic dashboard shape: time picker range on the inferred date AND a filter pill (match_phrase). */
    public void testTimeRangePlusFilterPill() throws Exception {
        String from = iso(3);
        String to = iso(27);
        assertSameIds(bool -> {
            bool.startArray("filter");
            bool.startObject().startObject("range").startObject("event_time");
            bool.field("gte", from).field("lte", to).field("format", "strict_date_optional_time");
            bool.endObject().endObject().endObject();
            bool.startObject().startObject("match_phrase").field("category", "login").endObject().endObject();
            bool.endArray();
        });
    }

    /** A one-sided time bound (open-ended "since") on the inferred date. */
    public void testOpenEndedTimeRange() throws Exception {
        String from = iso(15);
        assertSameIds(bool -> {
            bool.startArray("filter").startObject().startObject("range").startObject("event_time");
            bool.field("gte", from).field("format", "strict_date_optional_time");
            bool.endObject().endObject().endObject().endArray();
        });
    }

    /**
     * The renamed-timestamp path, which is what makes a dashboard time picker work on a dataset: a dataset defined with
     * a dynamic base plus a declared {@code @timestamp} rename of the source {@code event_time}. It asserts (1) the
     * dataset now exposes an {@code @timestamp} column — exactly what Kibana's {@code FROM <source> | LIMIT 0} probe
     * looks for — and (2) a request-filter range on that renamed {@code @timestamp} selects the same rows as the same
     * range on the index's underlying {@code event_time}, so the rename + filter are faithful end to end.
     */
    public void testRenamedTimestampResolvesAndFilters() throws Exception {
        assertThat("the renamed dataset must expose an @timestamp column", columnNames(RENAMED_DATASET), hasItem("@timestamp"));

        String from = iso(5);
        String to = iso(20);
        List<Object> fromDataset = selectedIds(RENAMED_DATASET, b -> rangeFilterOn(b, "@timestamp", from, to));
        List<Object> fromIndex = selectedIds(INDEX, b -> rangeFilterOn(b, "event_time", from, to));
        assertEquals("range on the renamed @timestamp must select the same rows as the index's event_time", fromIndex, fromDataset);
        assertThat("the filter must actually carve a subset", fromIndex.size(), greaterThan(0));
        assertThat("the filter must actually carve a subset", fromIndex.size(), lessThan(ROWS));
    }

    private static void rangeFilterOn(XContentBuilder b, String field, String from, String to) throws IOException {
        b.startArray("filter").startObject().startObject("range").startObject(field);
        b.field("gte", from).field("lte", to).field("format", "strict_date_optional_time");
        b.endObject().endObject().endObject().endArray();
    }

    @SuppressWarnings("unchecked")
    private List<String> columnNames(String source) throws IOException {
        Map<String, Object> resp = runEsqlSync(
            requestObjectBuilder().query("FROM " + source + " | LIMIT 0"),
            new AssertWarnings.NoWarnings(),
            null
        );
        List<Map<String, String>> cols = (List<Map<String, String>>) resp.get("columns");
        return cols.stream().map(c -> c.get("name")).toList();
    }

    /** Runs the same Kibana-shaped filter over the dataset and the index; the selected id sets must match and be non-trivial. */
    private void assertSameIds(CheckedConsumer<XContentBuilder, IOException> boolBody) throws Exception {
        List<Object> fromDataset = selectedIds(DATASET, boolBody);
        List<Object> fromIndex = selectedIds(INDEX, boolBody);
        assertEquals("dataset must select the same rows as the mirror index for the same request filter", fromIndex, fromDataset);
        assertThat("the filter must actually carve a subset", fromIndex.size(), greaterThan(0));
        assertThat("the filter must actually carve a subset", fromIndex.size(), lessThan(ROWS));
    }

    private List<Object> selectedIds(String source, CheckedConsumer<XContentBuilder, IOException> boolBody) throws IOException {
        var builder = requestObjectBuilder().query("FROM " + source + " | KEEP id | SORT id ASC").filter(b -> {
            b.startObject("bool");
            boolBody.accept(b);
            b.endObject();
        });
        Map<String, Object> response = runEsqlSync(builder, new AssertWarnings.NoWarnings(), null);
        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) response.get("values");
        List<Object> ids = new ArrayList<>(values.size());
        for (List<Object> row : values) {
            ids.add(row.get(0));
        }
        return ids;
    }

    private static String iso(int dayOffset) {
        return BASE.plus(dayOffset, ChronoUnit.DAYS).toString();
    }
}
