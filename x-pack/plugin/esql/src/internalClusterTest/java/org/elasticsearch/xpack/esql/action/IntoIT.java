/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.into.IntoStats;
import org.hamcrest.Matcher;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

/**
 * {@code INTO}: a query whose rows are written to an index instead of returned, answering with what it wrote. The
 * source holds {@code @timestamp}, {@code host} and {@code cpu}, every document on the same timestamp so that rows
 * collide wherever ids are derived from the data.
 */
public class IntoIT extends AbstractEsqlIntegTestCase {

    private static final String SOURCE = "source-into";
    private static final String DESTINATION = "dest-into";
    private static final long BASE_TIMESTAMP = 1_750_000_000_000L;

    private static final String SOURCE_MAPPING = """
        {
          "properties": {
            "@timestamp": { "type": "date" },
            "host":       { "type": "keyword" },
            "cpu":        { "type": "double" }
          }
        }
        """;

    private static final String ROLLUP_MAPPING = """
        {
          "properties": {
            "host":    { "type": "keyword" },
            "max_cpu": { "type": "double" },
            "samples": { "type": "long" }
          }
        }
        """;

    private static final String ROLLUP = "STATS max_cpu = MAX(cpu), samples = COUNT(*) BY host";

    /**
     * Index modes a destination is created in, picked at random per test so that every mode takes the same writes.
     * Time series is left out, since it needs dimensions and a routing path; it has tests of its own. The logsdb
     * modes require a {@code @timestamp} on every document, so they are only drawn for a destination that maps one.
     * A data stream cannot be in lookup mode, and lookup mode needs a single shard.
     */
    private static final List<String> INDEX_MODES = List.of("standard", "columnar", "lookup");
    private static final List<String> TIMESTAMP_INDEX_MODES = List.of("standard", "logsdb", "columnar", "lookup", "logsdb_columnar");
    private static final List<String> DATA_STREAM_MODES = List.of("standard", "logsdb", "columnar", "logsdb_columnar");

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), DataStreamsPlugin.class);
    }

    @Before
    public void createSource() {
        assumeTrue("INTO is behind the esql_into feature flag", EsqlCapabilities.Cap.INTO_COMMAND.isEnabled());
        assertAcked(indicesAdmin().prepareCreate(SOURCE).setMapping(SOURCE_MAPPING));
    }

    public void testWritesRowsAndReportsWhatItWrote() {
        indexHosts(3, 100);
        createDestination(ROLLUP_MAPPING);

        Map<String, Object> stats = into("FROM " + SOURCE + " | " + ROLLUP + " | INTO " + DESTINATION);
        assertThat(stats.get(IntoStats.TOTAL), equalTo(3L));
        assertThat(stats.get(IntoStats.CREATED), equalTo(3L));
        assertThat(stats.get(IntoStats.UPDATED), equalTo(0L));
        assertThat(stats.get(IntoStats.VERSION_CONFLICTS), equalTo(0L));
        assertThat(stats.get(IntoStats.BATCHES), equalTo(1L));
        assertThat(stats.get(IntoStats.FAILURES), equalTo(0L));
        assertThat(stats.get(IntoStats.FAILURE_MESSAGES), nullValue());

        assertThat("the rows are searchable right away, since the destination is refreshed by default", count(DESTINATION), equalTo(3L));
        assertThat(
            "every source document must be accounted for",
            number("FROM " + DESTINATION + " | STATS s = SUM(samples)"),
            equalTo(100L)
        );
    }

    /**
     * No row cap applies: a cap would make the write silently incomplete rather than truncate a display. The group
     * count sits above the default cap so that reinstating one fails this test.
     */
    public void testEveryRowIsWrittenHoweverManyThereAre() {
        final int hosts = 2500;
        indexHosts(hosts, hosts);
        createDestination(ROLLUP_MAPPING);

        Map<String, Object> stats = into("FROM " + SOURCE + " | " + ROLLUP + " | INTO " + DESTINATION);
        assertThat(stats.get(IntoStats.CREATED), equalTo((long) hosts));
        assertThat((Long) stats.get(IntoStats.BATCHES), greaterThanOrEqualTo(1L));
        assertThat(count(DESTINATION), equalTo((long) hosts));
    }

    /** Without ids of their own, rows are appended, so running the same query twice leaves both sets. */
    public void testRunningItAgainAppends() {
        indexHosts(3, 60);
        createDestination(ROLLUP_MAPPING);

        String query = "FROM " + SOURCE + " | " + ROLLUP + " | INTO " + DESTINATION;
        assertThat(into(query).get(IntoStats.CREATED), equalTo(3L));
        assertThat(into(query).get(IntoStats.CREATED), equalTo(3L));
        assertThat(count(DESTINATION), equalTo(6L));
    }

    public void testAnEmptyResultWritesNothing() {
        indexHosts(3, 30);
        createDestination(ROLLUP_MAPPING);

        Map<String, Object> stats = into("FROM " + SOURCE + " | WHERE host == \"absent\" | " + ROLLUP + " | INTO " + DESTINATION);
        assertThat(stats.get(IntoStats.TOTAL), equalTo(0L));
        assertThat(stats.get(IntoStats.CREATED), equalTo(0L));
        assertThat(stats.get(IntoStats.BATCHES), equalTo(0L));
        assertThat(count(DESTINATION), equalTo(0L));
    }

    /** A destination has to exist or be described by a template, so a mistyped name does not succeed. */
    public void testWritingToAMissingDestinationFails() {
        indexHosts(3, 10);
        assertFails(
            "FROM " + SOURCE + " | " + ROLLUP + " | INTO no-such-destination",
            containsString("[no-such-destination] does not exist and no index template matches it")
        );
    }

    /** The {@code _id} column, here the source's own ids, becomes the document id. */
    public void testIdColumnBecomesTheDocumentId() {
        indexHosts(3, 10);
        createDestination(SOURCE_MAPPING);

        Map<String, Object> stats = into("FROM " + SOURCE + " METADATA _id | KEEP _id, `@timestamp`, host, cpu | INTO " + DESTINATION);
        assertThat(stats.get(IntoStats.CREATED), equalTo(10L));
        assertThat(ids(DESTINATION), containsInAnyOrder(ids(SOURCE).toArray()));
    }

    public void testReplaceOverwritesCollidingDocuments() {
        indexHosts(3, 10);
        createDestination(SOURCE_MAPPING);
        assertThat(copySource("abort"), equalTo(10L));

        Map<String, Object> stats = into(
            "FROM "
                + SOURCE
                + " METADATA _id | EVAL cpu = cpu + 1000 | KEEP _id, `@timestamp`, host, cpu | INTO "
                + DESTINATION
                + " WITH { \"collisions\": \"replace\" }"
        );
        assertThat(stats.get(IntoStats.TOTAL), equalTo(10L));
        assertThat(stats.get(IntoStats.CREATED), equalTo(0L));
        assertThat(stats.get(IntoStats.UPDATED), equalTo(10L));
        assertThat(stats.get(IntoStats.VERSION_CONFLICTS), equalTo(0L));
        assertThat(count(DESTINATION), equalTo(10L));
        assertThat(number("FROM " + DESTINATION + " | STATS m = MIN(cpu)"), greaterThan(1000L));
    }

    public void testKeepLeavesCollidingDocumentsAlone() {
        indexHosts(3, 10);
        createDestination(SOURCE_MAPPING);
        assertThat(copySource("abort"), equalTo(10L));
        long maxCpu = number("FROM " + SOURCE + " | STATS m = MAX(cpu)");

        Map<String, Object> stats = into(
            "FROM "
                + SOURCE
                + " METADATA _id | EVAL cpu = cpu + 1000 | KEEP _id, `@timestamp`, host, cpu | INTO "
                + DESTINATION
                + " WITH { \"collisions\": \"keep\" }"
        );
        assertThat(stats.get(IntoStats.TOTAL), equalTo(10L));
        assertThat(stats.get(IntoStats.CREATED), equalTo(0L));
        assertThat(stats.get(IntoStats.UPDATED), equalTo(0L));
        assertThat(stats.get(IntoStats.VERSION_CONFLICTS), equalTo(10L));
        assertThat(stats.get(IntoStats.FAILURES), equalTo(0L));
        assertThat(count(DESTINATION), equalTo(10L));
        assertThat(number("FROM " + DESTINATION + " | STATS m = MAX(cpu)"), equalTo(maxCpu));
    }

    /** The default refuses to guess: a collision fails the query. */
    public void testCollisionsFailByDefault() {
        indexHosts(3, 10);
        createDestination(SOURCE_MAPPING);
        assertThat(copySource("abort"), equalTo(10L));

        assertFails(
            "FROM " + SOURCE + " METADATA _id | KEEP _id, `@timestamp`, host, cpu | INTO " + DESTINATION,
            containsString("already exists; use \"collisions\": \"replace\" or \"collisions\": \"keep\"")
        );
    }

    /** A computed id makes a rollup idempotent: the second run replaces the first run's documents. */
    public void testComputedIdMakesARollupIdempotent() {
        indexHosts(3, 30);
        createDestination(ROLLUP_MAPPING);

        String query = "FROM "
            + SOURCE
            + " | "
            + ROLLUP
            + " | EVAL _id = host | INTO "
            + DESTINATION
            + " WITH { \"collisions\": \"replace\" }";
        assertThat(into(query).get(IntoStats.CREATED), equalTo(3L));
        Map<String, Object> again = into(query);
        assertThat(again.get(IntoStats.CREATED), equalTo(0L));
        assertThat(again.get(IntoStats.UPDATED), equalTo(3L));
        assertThat(count(DESTINATION), equalTo(3L));
        assertThat(ids(DESTINATION), containsInAnyOrder("host-0", "host-1", "host-2"));
    }

    /**
     * The rows are searchable by the time the query returns: the destination is refreshed by default, even one that
     * never refreshes on its own.
     */
    public void testRowsAreSearchableByDefault() {
        indexHosts(3, 30);
        createDestination(ROLLUP_MAPPING, neverRefreshing());
        into("FROM " + SOURCE + " | " + ROLLUP + " | INTO " + DESTINATION);
        assertThat(count(DESTINATION), equalTo(3L));
    }

    /** With {@code refresh=false} the rows are written but left to the destination's own refresh, so none is visible yet. */
    public void testRefreshFalseLeavesRowsToTheDestination() {
        indexHosts(3, 30);
        createDestination(ROLLUP_MAPPING, neverRefreshing());
        Map<String, Object> stats = into("FROM " + SOURCE + " | " + ROLLUP + " | INTO " + DESTINATION + " WITH { \"refresh\": false }");
        assertThat(stats.get(IntoStats.CREATED), equalTo(3L));
        assertThat("nothing is searchable until the destination refreshes", count(DESTINATION), equalTo(0L));
        refresh(DESTINATION);
        assertThat(count(DESTINATION), equalTo(3L));
    }

    /** A column the destination does not map is added to its mapping when the mapping allows it. */
    public void testDynamicMappingAddsNewColumns() {
        indexHosts(3, 30);
        createDestination("""
            { "properties": { "host": { "type": "keyword" } } }
            """);

        Map<String, Object> stats = into("FROM " + SOURCE + " | " + ROLLUP + " | INTO " + DESTINATION);
        assertThat(stats.get(IntoStats.CREATED), equalTo(3L));
        assertThat(stats.get(IntoStats.FAILURES), equalTo(0L));
        assertThat(number("FROM " + DESTINATION + " | WHERE max_cpu > 0 AND samples == 10 | STATS c = COUNT(*)"), equalTo(3L));
    }

    /** A mapping that would refuse or silently drop a column fails the query before any row is written. */
    public void testMappingThatDoesNotAllowNewColumnsFailsBeforeWriting() {
        indexHosts(3, 30);
        for (String dynamic : new String[] { "strict", "false" }) {
            assertAcked(indicesAdmin().prepareDelete(DESTINATION + "*"));
            createDestination(Strings.format("""
                { "dynamic": "%s", "properties": { "host": { "type": "keyword" } } }
                """, dynamic));
            assertFails(
                "FROM " + SOURCE + " | " + ROLLUP + " | INTO " + DESTINATION,
                containsString(
                    "[max_cpu] is not mapped in ["
                        + DESTINATION
                        + "] and its mapping does not allow dynamic fields (dynamic="
                        + dynamic
                        + ")"
                )
            );
            refresh(DESTINATION);
            assertThat("nothing was written", count(DESTINATION), equalTo(0L));
        }
    }

    /** A row the destination refuses for a reason other than a collision fails the query, and nothing more is written. */
    public void testRefusedRowFailsTheQueryByDefault() {
        indexHosts(3, 30);
        createDestination("""
            { "properties": { "host": { "type": "keyword" }, "value": { "type": "double" } } }
            """);

        assertFails(
            "FROM "
                + SOURCE
                + " | EVAL value = CASE(host == \"host-0\", \"not a number\", cpu::keyword) | KEEP host, value | INTO "
                + DESTINATION,
            containsString("failed to parse field [value]")
        );
        assertFails(
            "FROM "
                + SOURCE
                + " | EVAL value = CASE(host == \"host-0\", \"not a number\", cpu::keyword) | KEEP host, value | INTO "
                + DESTINATION,
            containsString("use \"keep_going\": true to skip rows the destination refuses")
        );
    }

    /**
     * With {@code keep_going}, a refused row is counted and the rest is written. Every refusal is counted, but only
     * the first {@link IntoStats#MAX_FAILURE_MESSAGES} are described.
     */
    public void testKeepGoingReportsRefusedRowsAndWritesTheRest() {
        indexHosts(5, 50);
        createDestination("""
            { "properties": { "host": { "type": "keyword" }, "value": { "type": "double" } } }
            """);

        Map<String, Object> stats = into(
            "FROM "
                + SOURCE
                + " | EVAL value = CASE(host IN (\"host-0\", \"host-1\", \"host-2\"), \"not a number\", cpu::keyword)"
                + " | KEEP host, value | INTO "
                + DESTINATION
                + " WITH { \"keep_going\": true }"
        );
        assertThat(stats.get(IntoStats.TOTAL), equalTo(50L));
        assertThat(stats.get(IntoStats.CREATED), equalTo(20L));
        assertThat(stats.get(IntoStats.FAILURES), equalTo(30L));
        @SuppressWarnings("unchecked")
        List<String> messages = (List<String>) stats.get(IntoStats.FAILURE_MESSAGES);
        assertThat("only the first few refusals are described", messages, hasSize(IntoStats.MAX_FAILURE_MESSAGES));
        assertThat(messages.get(0), containsString(DESTINATION + "/"));
        assertThat(messages.get(0), containsString("failed to parse field [value]"));
        assertThat(count(DESTINATION), equalTo(20L));
    }

    /**
     * A data stream only takes new documents by name and generates their ids, so an {@code _id} column and
     * {@code collisions=replace} are refused up front, and every run adds its rows.
     */
    public void testDataStreamGeneratesItsOwnIds() throws IOException {
        indexHosts(3, 10);
        String stream = "stream-into";
        createDataStream(stream, SOURCE_MAPPING);

        assertFails(
            "FROM " + SOURCE + " METADATA _id | KEEP _id, `@timestamp`, host, cpu | INTO " + stream,
            containsString("[_id] cannot be written to [" + stream + "]: a data stream generates its own document ids")
        );
        assertThat(count(stream), equalTo(0L));

        String copy = "FROM " + SOURCE + " | KEEP `@timestamp`, host, cpu | INTO " + stream;
        assertFails(
            copy + " WITH { \"collisions\": \"replace\" }",
            containsString(
                "["
                    + stream
                    + "] is a data stream, which only takes new documents, so INTO cannot replace in it; use \"collisions\": \"keep\""
            )
        );
        assertThat(count(stream), equalTo(0L));

        for (String collisions : List.of("", " WITH { \"collisions\": \"keep\" }")) {
            Map<String, Object> stats = into(copy + collisions);
            assertThat(stats.get(IntoStats.CREATED), equalTo(10L));
            assertThat(stats.get(IntoStats.UPDATED), equalTo(0L));
            assertThat(stats.get(IntoStats.VERSION_CONFLICTS), equalTo(0L));
        }
        assertThat(count(stream), equalTo(20L));
    }

    /**
     * A time series data stream derives the id from the dimensions and the timestamp on the shard, so rows for the same
     * host at the same time collide without an {@code _id} column; like any data stream, it cannot be replaced into.
     */
    public void testTimeSeriesDataStreamCollidesOnDerivedIds() throws IOException {
        String stream = "tsds-into";
        createTimeSeriesDataStream(stream);
        // Time series backing indices only accept timestamps near now.
        indexHosts(3, 30, System.currentTimeMillis());

        Map<String, Object> kept = into(
            "FROM " + SOURCE + " | KEEP `@timestamp`, host, cpu | INTO " + stream + " WITH { \"collisions\": \"keep\" }"
        );
        assertThat(kept.get(IntoStats.CREATED), equalTo(3L));
        assertThat(kept.get(IntoStats.VERSION_CONFLICTS), equalTo(27L));
        assertThat(count(stream), equalTo(3L));

        assertFails(
            "FROM " + SOURCE + " | KEEP `@timestamp`, host, cpu | INTO " + stream + " WITH { \"collisions\": \"replace\" }",
            containsString("[" + stream + "] is a data stream, which only takes new documents, so INTO cannot replace in it")
        );
    }

    /**
     * A time series index derives ids from the dimensions and the timestamp, so rows collide without an {@code _id}
     * column: every document of a host shares the timestamp, and only the first per host gets in.
     */
    public void testTimeSeriesDestinationDerivesItsOwnIds() {
        indexHosts(3, 30);
        String tsdb = "tsdb-into";
        assertAcked(
            indicesAdmin().prepareCreate(tsdb)
                .setSettings(
                    Settings.builder()
                        .put("index.mode", "time_series")
                        .put("index.routing_path", "host")
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                )
                .setMapping("""
                    {
                      "properties": {
                        "@timestamp": { "type": "date" },
                        "host":       { "type": "keyword", "time_series_dimension": true },
                        "cpu":        { "type": "double", "time_series_metric": "gauge" }
                      }
                    }
                    """)
        );

        Map<String, Object> stats = into(
            "FROM " + SOURCE + " | KEEP `@timestamp`, host, cpu | INTO " + tsdb + " WITH { \"collisions\": \"keep\" }"
        );
        assertThat(stats.get(IntoStats.TOTAL), equalTo(30L));
        assertThat(stats.get(IntoStats.CREATED), equalTo(3L));
        assertThat(stats.get(IntoStats.VERSION_CONFLICTS), equalTo(27L));
        assertThat(stats.get(IntoStats.FAILURES), equalTo(0L));
        assertThat(count(tsdb), equalTo(3L));

        assertFails("FROM " + SOURCE + " | KEEP `@timestamp`, host, cpu | INTO " + tsdb, containsString("already exists"));

        // A standalone time series index is not append-only: a replace overwrites the document for the host at that time.
        Map<String, Object> replaced = into(
            "FROM "
                + SOURCE
                + " | EVAL cpu = cpu + 1000 | KEEP `@timestamp`, host, cpu | INTO "
                + tsdb
                + " WITH { \"collisions\": \"replace\" }"
        );
        assertThat(replaced.get(IntoStats.CREATED), equalTo(0L));
        assertThat(replaced.get(IntoStats.UPDATED), equalTo(30L));
        assertThat(count(tsdb), equalTo(3L));
        assertThat(number("FROM " + tsdb + " | STATS m = MIN(cpu)"), greaterThan(1000L));
    }

    public void testUnlimitedSortIsRefused() {
        indexHosts(3, 10);
        createDestination(SOURCE_MAPPING);
        assertFails(
            "FROM " + SOURCE + " | SORT host | KEEP `@timestamp`, host, cpu | INTO " + DESTINATION,
            containsString("SORT has no effect on what INTO writes")
        );
    }

    public void testMetadataColumnsOtherThanIdAreRefused() {
        indexHosts(3, 10);
        createDestination(SOURCE_MAPPING);
        assertFails(
            "FROM " + SOURCE + " METADATA _index | KEEP _index, host | INTO " + DESTINATION,
            containsString("metadata column [_index] cannot be written by INTO; drop it with DROP or KEEP")
        );
    }

    /**
     * Loading every unmapped field adds a {@code _unmapped_fields} column holding a JSON object per document, which INTO
     * would write as one object field rather than as the fields it stands for, so it is refused.
     */
    public void testLoadingAllUnmappedFieldsIsRefused() {
        assumeTrue("requires SET unmapped_fields", EsqlCapabilities.Cap.OPTIONAL_FIELDS_LOAD_ALL_V2.isEnabled());
        indexHosts(3, 10);
        createDestination(SOURCE_MAPPING);
        assertFails(
            "SET unmapped_fields=\"load_all\"; FROM " + SOURCE + " | KEEP `@timestamp`, host, cpu | INTO " + DESTINATION,
            containsString("INTO cannot be used with [unmapped_fields=LOAD_ALL]")
        );
    }

    /** An estimate is something to display, not to persist with no sign that it was estimated, unless asked for. */
    public void testApproximationNeedsOptingIn() {
        assumeTrue("requires SET approximation", EsqlCapabilities.Cap.APPROXIMATION_V7.isEnabled());
        indexHosts(3, 10);
        createDestination(ROLLUP_MAPPING);
        assertFails(
            "SET approximation=true; FROM " + SOURCE + " | " + ROLLUP + " | INTO " + DESTINATION,
            containsString("INTO cannot be used with approximation unless \"approximate\": true")
        );
        refresh(DESTINATION);
        assertThat("nothing was written", count(DESTINATION), equalTo(0L));

        Map<String, Object> stats = into(
            "SET approximation=true; FROM " + SOURCE + " | " + ROLLUP + " | INTO " + DESTINATION + " WITH { \"approximate\": true }"
        );
        assertThat((Long) stats.get(IntoStats.CREATED), greaterThan(0L));
        assertThat(count(DESTINATION), equalTo(stats.get(IntoStats.CREATED)));
    }

    public void testForkIsRefused() {
        indexHosts(3, 10);
        createDestination(SOURCE_MAPPING);
        assertFails(
            "FROM " + SOURCE + " | FORK (WHERE cpu > 50) (WHERE cpu <= 50) | KEEP `@timestamp`, host, cpu | INTO " + DESTINATION,
            containsString("INTO is not supported in a query with FORK or UNION branches")
        );
    }

    /** Writes the source's documents, ids included, to the destination; returns how many were created. */
    private long copySource(String collisions) {
        return (Long) into(
            "FROM "
                + SOURCE
                + " METADATA _id | KEEP _id, `@timestamp`, host, cpu | INTO "
                + DESTINATION
                + " WITH { \"collisions\": \""
                + collisions
                + "\" }"
        ).get(IntoStats.CREATED);
    }

    /** Runs an {@code INTO} query and returns its one row, keyed by column. */
    protected Map<String, Object> into(String query) {
        try (EsqlQueryResponse response = run(syncEsqlQueryRequest(query))) {
            List<String> columns = response.columns().stream().map(ColumnInfoImpl::name).toList();
            assertThat(columns, equalTo(IntoStats.ATTRIBUTES.stream().map(a -> a.name()).toList()));
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            Map<String, Object> stats = new HashMap<>();
            Iterator<Object> values = rows.get(0).iterator();
            for (String column : columns) {
                stats.put(column, values.next());
            }
            return stats;
        }
    }

    private void assertFails(String query, Matcher<String> message) {
        Exception failure = expectThrows(Exception.class, () -> into(query));
        List<String> messages = new ArrayList<>();
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            messages.add(cause.getMessage());
        }
        assertThat("no cause of " + failure + " matched", messages.stream().anyMatch(message::matches), equalTo(true));
    }

    private long number(String query) {
        try (EsqlQueryResponse response = run(syncEsqlQueryRequest(query))) {
            return ((Number) getValuesList(response).get(0).get(0)).longValue();
        }
    }

    private long count(String index) {
        return number("FROM " + index + " | STATS c = COUNT(*)");
    }

    private List<String> ids(String index) {
        try (EsqlQueryResponse response = run(syncEsqlQueryRequest("FROM " + index + " METADATA _id | KEEP _id"))) {
            return getValuesList(response).stream().map(row -> (String) row.get(0)).toList();
        }
    }

    /**
     * Settings for a destination whose rows become visible only when INTO (or the test) refreshes it. A replica that
     * finishes recovering after the write opens a searcher that already holds the rows, so there is none.
     */
    private static Settings neverRefreshing() {
        return Settings.builder()
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "-1")
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
    }

    private void createDestination(String mapping) {
        createDestination(mapping, Settings.EMPTY);
    }

    private void createDestination(String mapping, Settings settings) {
        String mode = randomFrom(mapping.contains("@timestamp") ? TIMESTAMP_INDEX_MODES : INDEX_MODES);
        Settings.Builder withMode = Settings.builder().put(settings).put(IndexSettings.MODE.getKey(), mode);
        if (mode.equals("lookup")) {
            withMode.put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1);
        }
        assertAcked(indicesAdmin().prepareCreate(DESTINATION).setSettings(withMode).setMapping(mapping));
    }

    private void indexHosts(int hosts, int documents) {
        indexHosts(hosts, documents, BASE_TIMESTAMP);
    }

    private void indexHosts(int hosts, int documents, long timestamp) {
        final BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < documents; i++) {
            bulk.add(
                client().prepareIndex(SOURCE)
                    .setOpType(DocWriteRequest.OpType.CREATE)
                    .setSource("@timestamp", timestamp, "host", "host-" + (i % hosts), "cpu", (double) randomIntBetween(1, 100))
            );
        }
        final BulkResponse response = bulk.get();
        assertFalse(response.buildFailureMessage(), response.hasFailures());
        refresh(SOURCE);
    }

    private void createDataStream(String name, String mappings) throws IOException {
        String mode = randomFrom(DATA_STREAM_MODES);
        createDataStream(
            name,
            mappings,
            Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put(IndexSettings.MODE.getKey(), mode)
                .build()
        );
    }

    private void createTimeSeriesDataStream(String name) throws IOException {
        createDataStream(
            name,
            """
                {
                  "properties": {
                    "@timestamp": { "type": "date" },
                    "host":       { "type": "keyword", "time_series_dimension": true },
                    "cpu":        { "type": "double", "time_series_metric": "gauge" }
                  }
                }
                """,
            Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put("index.mode", "time_series")
                .put("index.routing_path", "host")
                .build()
        );
    }

    private void createDataStream(String name, String mappings, Settings settings) throws IOException {
        final ComposableIndexTemplate template = ComposableIndexTemplate.builder()
            .indexPatterns(List.of(name + "*"))
            .template(Template.builder().settings(settings).mappings(CompressedXContent.fromJSON(mappings)))
            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
            .build();
        final var request = new TransportPutComposableIndexTemplateAction.Request(name + "-template");
        request.indexTemplate(template);
        assertAcked(client().execute(TransportPutComposableIndexTemplateAction.TYPE, request));
        assertAcked(
            client().execute(
                CreateDataStreamAction.INSTANCE,
                new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, name)
            )
        );
    }
}
