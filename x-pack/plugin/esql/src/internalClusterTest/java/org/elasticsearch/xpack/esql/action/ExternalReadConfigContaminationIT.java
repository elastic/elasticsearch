/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.cluster.metadata.DatasetFieldMapping;
import org.elasticsearch.cluster.metadata.DatasetMapping;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.dataset.DeleteDatasetAction;
import org.elasticsearch.xpack.esql.datasources.dataset.PutDatasetAction;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.junit.After;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

/**
 * End-to-end pin for the read-configuration identity: one dataset must never be served a row count that a differently-read
 * dataset over the same file produced.
 *
 * <p>The sequence has to be ASYMMETRIC, which is why the obvious test does not work. {@code COUNT(*)} projects no
 * columns, so a declared column is never parsed, nothing fails to coerce and no row drops — two datasets both asking
 * {@code COUNT(*)} legitimately agree, with or without the fix. And a per-column aggregate on the declared side is
 * already safe-missed by the declared-overlay poison that predates this work, so asserting on one proves nothing
 * about this feature either.
 *
 * <p>What manifests the defect is a declared read that PARSES the column — dropping the row that will not coerce and
 * publishing the short count — followed by a plain {@code COUNT(*)} on the inferred dataset, which is neither
 * projected nor poisoned and so serves whatever the entry holds. Before the resolved read configuration entered the stats identity
 * both datasets addressed one entry and the inferred dataset answered with the declared read's count.
 */
// Single node pins the coordinator: the warm cache is a per-node singleton, so a random coordinator would let the
// second query land on an empty cache and re-scan, which passes for the wrong reason.
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class ExternalReadConfigContaminationIT extends AbstractExternalDataSourceIT {

    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(60);
    private static final int ROWS = 200;
    private static final int BAD_ROW = ROWS / 2;
    private static final String SRC = "contamination_src";

    private final List<String> datasets = new ArrayList<>();

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    @After
    public void cleanupDatasets() {
        for (String dataset : datasets) {
            try {
                client().execute(DeleteDatasetAction.INSTANCE, new DeleteDatasetAction.Request(TIMEOUT, TIMEOUT, new String[] { dataset }))
                    .actionGet();
            } catch (Exception ignored) {
                // best-effort teardown, mirroring the base cleanupRegistry()
            }
        }
    }

    /**
     * The reachable wrong answer. Two datasets over one file: one declares {@code ts} with a day-before-month dialect,
     * the other infers ISO. Every value parses cleanly under BOTH dialects — to DIFFERENT instants. So neither read
     * fails, neither drops a row, and nothing older safe-misses: the declared read simply harvests a {@code MIN(ts)}
     * that is a correct answer for itself and a wrong one for its neighbour.
     * <p>
     * That is why the count-based constructions do not work here. A dropped row is caught by the pinned-contribution
     * strip long before the cache sees it, and {@code COUNT(*)} parses no column at all. A same-type dialect
     * difference is the one read configuration that reaches the cache with two legitimately different measurements.
     */
    public void testDeclaredDialectDoesNotPoisonTheInferredExtremum() throws Exception {
        String uri = writeFixture();
        String declared = register("dialect_declared", uri, mappingTsWithDialect());
        String inferred = register("dialect_inferred", uri, null);

        // Day-before-month: 2024-03-02 reads as 3 February, so the earliest instant is the row holding 2024-12-01.
        assertMin(declared, "2024-01-12");
        // ISO: the same bytes read as 2 March, and the earliest is the row holding 2024-01-05.
        assertMin(inferred, "2024-01-05");
    }

    /**
     * The same crossing in the other warm order. Weaker than its twin by construction: a declared dataset does not
     * warm today — its contributions are classified as a union_by_name pin and stripped before commit — so this one
     * passes with the gates disabled too. It is kept as a correctness pin against the day declared reads do warm,
     * not as evidence the gate works. Its twin above is the discriminating one.
     */
    public void testInferredDialectDoesNotPoisonTheDeclaredExtremum() throws Exception {
        String uri = writeFixture();
        String declared = register("reverse_declared", uri, mappingTsWithDialect());
        String inferred = register("reverse_inferred", uri, null);

        assertMin(inferred, "2024-01-05");
        assertMin(declared, "2024-01-12");
    }

    /**
     * The reproduction measured on {@code main}: a STRICT declared dataset poisons an inferred sibling's row count.
     *
     * <p>Two details make it work where every other construction failed. The poisoner must be STRICT — a non-strict
     * overlay's harvest is classified as a union_by_name pin and stripped before commit, while a strict read has no
     * such pins and its contribution reaches the cache untouched. And the victim must run FIRST: the reconcile only
     * enriches entries that already exist, so a poisoner-first ordering seeds nothing and quietly self-heals.
     *
     * <p>On main the final query answers 199 with nothing scanned — the poisoner's count, served to a dataset whose
     * own answer is 200.
     *
     * <p>Read this as a correctness pin rather than as evidence the read-configuration gate works. The poisoner's
     * drop is a coercion failure of a projected column, which is projection-dependent, so its publish is now
     * suppressed at the producer and there is no longer a foreign count for the gate to refuse. It passes with the
     * gate disabled. {@link #testDeclaredDialectDoesNotPoisonTheInferredExtremum} drops no rows by construction and
     * remains the gate's discriminator.
     */
    public void testStrictDeclaredReadDoesNotPoisonTheInferredCount() throws Exception {
        String uri = writeDropFixture();
        String victim = register("strict_victim", uri, null, true);
        String poisoner = register("strict_poisoner", uri, strictAgeAsInteger(), true);

        // Twice: the entry must be warm, not merely seeded. The reconcile only enriches entries that already hold
        // statistics, which is why a poisoner-first ordering quietly self-heals and proves nothing.
        assertCount(victim, "STATS c = COUNT(*)", ROWS);
        assertCount(victim, "STATS c = COUNT(*)", ROWS);
        // Parses age and drops the row that will not coerce. On main that survivor count reached the shared entry;
        // now the drop is recognised as projection-dependent and the publish is suppressed before it gets there.
        assertCount(poisoner, "STATS c = COUNT(*), hi = MAX(age)", ROWS - 1L);
        // Its own answer is every row; it must not inherit the poisoner's.
        assertCount(victim, "STATS c = COUNT(*)", ROWS);
    }

    private void assertCount(String dataset, String statsClause, long expected) {
        String query = "FROM " + dataset + " | " + statsClause;
        try (var response = run(syncEsqlQueryRequest(query).profile(true), TIMEOUT)) {
            assertThat(query, ((Number) getValuesList(response).get(0).get(0)).longValue(), equalTo(expected));
        }
    }

    /**
     * Plain header, not a typed one: the column must be INFERRED as text, and the value that will not coerce sits
     * early enough that schema sampling sees it. A typed header would bind the schema a different way and take the
     * read down another rail entirely.
     */
    private String writeDropFixture() throws Exception {
        StringBuilder sb = new StringBuilder("name,age\n");
        for (int i = 0; i < ROWS; i++) {
            sb.append("row_").append(i).append(',').append(i == 3 ? "oops" : String.valueOf(i * 10)).append('\n');
        }
        Path file = createTempDir().resolve("drops.csv");
        Files.writeString(file, sb.toString());
        return StoragePath.fileUri(file);
    }

    private static DatasetMapping strictAgeAsInteger() {
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("name", new DatasetFieldMapping("keyword", null));
        properties.put("age", new DatasetFieldMapping("integer", null));
        // Dynamic.FALSE == strict: the declaration is the whole schema, and such a read carries no union_by_name
        // pins, so nothing strips its harvest on the way to the cache.
        return new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, properties));
    }

    /** Asserts the date-only prefix of {@code MIN(ts)}, which is what the two dialects disagree about. */
    private void assertMin(String dataset, String expectedDatePrefix) {
        String query = "FROM " + dataset + " | STATS lo = MIN(ts)";
        try (var response = run(syncEsqlQueryRequest(query).profile(true), TIMEOUT)) {
            String actual = String.valueOf(getValuesList(response).get(0).get(0));
            assertThat(query, actual.startsWith(expectedDatePrefix), equalTo(true));
        }
    }

    /**
     * Two columns via a typed header, which pins {@code age} to text so inference cannot accidentally agree with the
     * declaration. If {@code age} inferred as a number, both datasets would drop the same row and the test would pass
     * while proving nothing.
     */
    private String writeFixture() throws Exception {
        // Every day-of-month is <= 12, so each value parses under BOTH dialects — no read fails, no row drops, and
        // the only difference between the two datasets is which instant the same bytes mean.
        // ts is ALREADY datetime for the inferred side, so the declaration changes only the DIALECT, not the type.
        // A retype (keyword -> datetime) would be classified as a union_by_name pin and stripped from the harvest
        // before the cache ever sees it, which is why the retype construction cannot reach this defect.
        StringBuilder sb = new StringBuilder("id:integer,ts:datetime\n");
        String[] dates = { "2024-03-02", "2024-01-05", "2024-12-01", "2024-07-08", "2024-05-11" };
        for (int i = 0; i < ROWS; i++) {
            sb.append(i).append(',').append(dates[i % dates.length]).append("T00:00:00").append('\n');
        }
        Path file = createTempDir().resolve("dialects.csv");
        Files.writeString(file, sb.toString());
        return StoragePath.fileUri(file);
    }

    private static DatasetMapping mappingTsWithDialect() {
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("ts", DatasetFieldMapping.withFormat("datetime", null, "yyyy-dd-MM'T'HH:mm:ss"));
        return new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.TRUE, properties));
    }

    private String registerWithSettings(String name, String uri, DatasetMapping mapping, Map<String, Object> settings) {
        registerDataSource(SRC, Map.of());
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(TIMEOUT, TIMEOUT, name, SRC, uri, null, new LinkedHashMap<>(settings), mapping)
            )
        );
        datasets.add(name);
        return name;
    }

    private String register(String name, String uri, DatasetMapping mapping) {
        return register(name, uri, mapping, true);
    }

    private String register(String name, String uri, DatasetMapping mapping, boolean skipRow) {
        registerDataSource(SRC, Map.of()); // idempotent-per-test; the base tracks and tears down the source
        Map<String, Object> settings = new LinkedHashMap<>();
        settings.put("format", "csv");
        if (skipRow) {
            settings.put("error_mode", "skip_row");
        }
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(TIMEOUT, TIMEOUT, name, SRC, uri, null, settings, mapping)
            )
        );
        datasets.add(name);
        return name;
    }
}
