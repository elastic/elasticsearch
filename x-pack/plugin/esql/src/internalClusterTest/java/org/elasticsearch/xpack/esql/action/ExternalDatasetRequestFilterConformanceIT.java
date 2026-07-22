/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.metadata.DatasetFieldMapping;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * The out-of-band request {@code filter} is applied to an external dataset by translating the Query DSL into ES|QL
 * predicates inserted above the dataset leaf, where the same filter on an index is applied as a Lucene query on the
 * index fragment. Those are two entirely different evaluation paths; this suite is the differential proof that they
 * <em>mean the same thing</em>.
 *
 * <p>The exact same rows are loaded twice — once as a mapped index, once as a strict declared-schema CSV dataset with
 * column types matching the index mapping — and every case runs one DSL filter against both, asserting the set of
 * {@code id}s each selects is identical. If the translation diverges from the index semantics for any construct (the
 * {@code minimum_should_match} edge, integral narrowing, date round-up and {@code now} math, missing-field leniency),
 * one of these fails with the offending filter in the message.
 *
 * <p>Fields are single-valued here so the any-value reduction the translator emits ({@code mv_contains} and friends)
 * coincides with scalar equality; the multivalue any-value semantics are pinned by the translator's unit tests.
 */
public class ExternalDatasetRequestFilterConformanceIT extends AbstractExternalDataSourceIT {

    private static final int ROWS = 40;
    private static final String INDEX = "conf_idx";
    // Non-midnight so a coarse day-precision bound actually exercises rounding: lte "2020-01-20" must round UP to the
    // end of the day to include a 12:34:56 row — a naive midnight parse would drop it, diverging from the index.
    private static final Instant BASE = Instant.parse("2020-01-01T12:34:56Z");

    private String dataset;

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    /** {@code id}, {@code status}, {@code bytes} at row i cycle so filters carve non-trivial, predictable subsets. */
    private static int status(int i) {
        return 200 + (i % 3) * 100; // 200, 300, 400
    }

    private static String tag(int i) {
        return "t" + (i % 4); // t0..t3
    }

    private static long bytes(int i) {
        return i * 1000L;
    }

    private static String ts(int i) {
        return DateTimeFormatter.ISO_INSTANT.format(BASE.plus(Duration.ofDays(i))); // 12:34:56 on 2020-01-(i+1)
    }

    /** A keyword whose stored values are genuinely mixed-case, so a case-insensitive match exercises the field-side fold. */
    private static String label(int i) {
        return new String[] { "Alpha", "BETA", "gamma", "DeLtA" }[i % 4];
    }

    @Before
    public void loadBothSources() throws Exception {
        // The index: one shard so the result order is trivial to reason about; ESQL sorts explicitly anyway.
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(INDEX)
                .setSettings(Settings.builder().put("index.number_of_shards", 1))
                .setMapping(
                    "id",
                    "type=integer",
                    "status",
                    "type=integer",
                    "tags",
                    "type=keyword",
                    "bytes",
                    "type=long",
                    "ts",
                    "type=date",
                    "label",
                    "type=keyword"
                )
        );
        for (int i = 0; i < ROWS; i++) {
            client().prepareIndex(INDEX)
                .setSource("id", i, "status", status(i), "tags", tag(i), "bytes", bytes(i), "ts", ts(i), "label", label(i))
                .get();
        }
        client().admin().indices().prepareRefresh(INDEX).get();

        // The dataset: identical rows as a strict declared-schema CSV, types matching the index mapping exactly.
        StringBuilder csv = new StringBuilder("id:integer,status:integer,tags:keyword,bytes:long,ts:date,label:keyword\n");
        for (int i = 0; i < ROWS; i++) {
            csv.append(i)
                .append(',')
                .append(status(i))
                .append(',')
                .append(tag(i))
                .append(',')
                .append(bytes(i))
                .append(',')
                .append(ts(i))
                .append(',')
                .append(label(i))
                .append('\n');
        }
        Path csvFile = createTempDir().resolve("conformance.csv");
        Files.writeString(csvFile, csv.toString(), StandardCharsets.UTF_8);
        dataset = registerStrictDataset("conf_ds", StoragePath.fileUri(csvFile), declaredColumns(), Map.of("format", "csv"));
    }

    private static LinkedHashMap<String, DatasetFieldMapping> declaredColumns() {
        LinkedHashMap<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("id", new DatasetFieldMapping("integer", null));
        properties.put("status", new DatasetFieldMapping("integer", null));
        properties.put("tags", new DatasetFieldMapping("keyword", null));
        properties.put("bytes", new DatasetFieldMapping("long", null));
        properties.put("ts", new DatasetFieldMapping("date", null));
        properties.put("label", new DatasetFieldMapping("keyword", null));
        return properties;
    }

    /** The heart of the suite: the same request filter must select the identical id set on the index and the dataset. */
    private void assertSelectsSameRows(QueryBuilder filter) {
        List<Object> fromIndex = selectedIds(INDEX, filter);
        List<Object> fromDataset = selectedIds(dataset, filter);
        assertEquals("filter must select identical rows on index and dataset: " + filter, fromIndex, fromDataset);
    }

    private List<Object> selectedIds(String source, QueryBuilder filter) {
        EsqlQueryRequest request = syncEsqlQueryRequest("FROM " + source + " | KEEP id | SORT id ASC").filter(filter);
        try (EsqlQueryResponse response = run(request, TIMEOUT)) {
            return getValuesList(response).stream().map(row -> row.get(0)).toList();
        }
    }

    public void testTermOnInteger() {
        assertSelectsSameRows(QueryBuilders.termQuery("status", 300));
    }

    public void testTermOnKeyword() {
        assertSelectsSameRows(QueryBuilders.termQuery("tags", "t2"));
    }

    /** A case-insensitive keyword term matches regardless of case — an uppercase query hits the lowercase values on both paths. */
    public void testCaseInsensitiveTermOnKeyword() {
        assertSelectsSameRows(QueryBuilders.termQuery("tags", "T2").caseInsensitive(true));
    }

    /** A case-insensitive keyword term with no case-folding match selects nothing on both paths — never a silent over-match. */
    public void testCaseInsensitiveTermNoMatch() {
        assertSelectsSameRows(QueryBuilders.termQuery("tags", "T9").caseInsensitive(true));
    }

    /** Exercises the field-side fold: a lower-case term matches genuinely mixed-case STORED values (e.g. "BETA") on both paths. */
    public void testCaseInsensitiveTermMatchesStoredMixedCase() {
        assertSelectsSameRows(QueryBuilders.termQuery("label", "beta").caseInsensitive(true));
    }

    /** A decimal against an integral field matches nothing on both paths — never a truncated match (B2). */
    public void testDecimalTermOnIntegerMatchesNothing() {
        assertSelectsSameRows(QueryBuilders.termQuery("status", 300.5));
    }

    public void testTermsOnInteger() {
        assertSelectsSameRows(QueryBuilders.termsQuery("status", List.of(200, 400)));
    }

    /** An unmatchable decimal is dropped from the set; the remaining integral values still match (B2). */
    public void testTermsWithUnmatchableDecimalOnInteger() {
        assertSelectsSameRows(QueryBuilders.termsQuery("status", List.of(200, 300.5, 400)));
    }

    public void testRangeOnLongBothBounds() {
        assertSelectsSameRows(QueryBuilders.rangeQuery("bytes").gte(5_000).lt(25_000));
    }

    public void testRangeOnIntegerOneSided() {
        assertSelectsSameRows(QueryBuilders.rangeQuery("status").gt(200));
    }

    /** A fractional bound on an integral field rounds inward exactly like the index — never truncates and over-matches. */
    public void testFractionalIntegerRangeBoundRoundsInwardLikeIndex() {
        assertSelectsSameRows(QueryBuilders.rangeQuery("status").gte(300.5)); // -> >= 301 (the 400s), not >= 300
        assertSelectsSameRows(QueryBuilders.rangeQuery("status").lte(300.5)); // -> <= 300 (200s and 300s)
        assertSelectsSameRows(QueryBuilders.rangeQuery("status").gte(200.5).lte(400.5)); // both ends inward
    }

    /** Coarse (day-precision) date bounds round to the edges of their unit identically on both paths (B3). */
    public void testDateRangeCoarseInclusiveBounds() {
        assertSelectsSameRows(QueryBuilders.rangeQuery("ts").gte("2020-01-05").lte("2020-01-20"));
    }

    /** Exclusive date bounds nudge one unit inward after rounding, identically on both paths (B3). */
    public void testDateRangeCoarseExclusiveBounds() {
        assertSelectsSameRows(QueryBuilders.rangeQuery("ts").gt("2020-01-05").lt("2020-01-20"));
    }

    /** {@code now} date math resolves against the one query start time both paths share, so they agree (B3). */
    public void testDateRangeNowMathAgrees() {
        assertSelectsSameRows(QueryBuilders.rangeQuery("ts").lte("now")); // all 2020 rows precede now
        assertSelectsSameRows(QueryBuilders.rangeQuery("ts").gte("now")); // none do
        assertSelectsSameRows(QueryBuilders.rangeQuery("ts").gte("now-9000d")); // ~1995 — all rows
    }

    public void testExists() {
        assertSelectsSameRows(QueryBuilders.existsQuery("tags"));
    }

    public void testBoolMustWithShould() {
        assertSelectsSameRows(
            QueryBuilders.boolQuery()
                .must(QueryBuilders.rangeQuery("bytes").gte(3_000))
                .should(QueryBuilders.termQuery("status", 200))
                .should(QueryBuilders.termQuery("status", 400))
        );
    }

    /** A should-only bool defaults to requiring one clause on both paths. */
    public void testShouldOnlyBool() {
        assertSelectsSameRows(
            QueryBuilders.boolQuery().should(QueryBuilders.termQuery("status", 200)).should(QueryBuilders.termQuery("tags", "t1"))
        );
    }

    /** minimum_should_match=0 with a must present drops the should to optional on both paths (B1). */
    public void testMinimumShouldMatchZeroWithMust() {
        assertSelectsSameRows(
            QueryBuilders.boolQuery()
                .must(QueryBuilders.rangeQuery("bytes").gte(10_000))
                .should(QueryBuilders.termQuery("status", 200))
                .minimumShouldMatch(0)
        );
    }

    /** minimum_should_match=0 on a should-ONLY bool still requires one clause on both paths — it is not match-all (B1). */
    public void testMinimumShouldMatchZeroShouldOnly() {
        assertSelectsSameRows(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.termQuery("status", 300))
                .should(QueryBuilders.termQuery("status", 400))
                .minimumShouldMatch(0)
        );
    }

    /** A term on a field neither source has matches nothing on both — unmapped index field and missing dataset field agree. */
    public void testMissingFieldUnderConjunctionMatchesNothing() {
        assertSelectsSameRows(QueryBuilders.termQuery("nope", "x"));
    }

    /**
     * An EXCLUSIVE range over a field neither source has matches nothing on both. The dataset used to degrade the whole
     * filter to unfiltered here (returning every row) where the index's unmapped-field range matches none.
     */
    public void testMissingFieldExclusiveRangeMatchesNothing() {
        assertSelectsSameRows(QueryBuilders.rangeQuery("nope").gte(0).lt(10));
    }

    /**
     * One bad clause must not sink the whole filter. A present term AND an exclusive range over a missing field: the
     * missing leg folds to false, so both select nothing. Before the fix the range threw and degraded the entire
     * filter, so the dataset returned every row while the index returned none.
     */
    public void testConjunctionWithMissingExclusiveRangeSelectsNothing() {
        assertSelectsSameRows(
            QueryBuilders.boolQuery().must(QueryBuilders.termQuery("status", 300)).must(QueryBuilders.rangeQuery("nope").gte(0).lt(10))
        );
    }

    /** A negated term on a field neither source has matches everything on both — the leniency the translation reproduces. */
    public void testNegatedMissingFieldMatchesEverything() {
        assertSelectsSameRows(QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("nope", "x")));
    }

    /** A conjunction that mixes a present and a missing field: the missing leg drops the whole clause on both. */
    public void testConjunctionWithMissingFieldDropsClause() {
        assertSelectsSameRows(
            QueryBuilders.boolQuery().must(QueryBuilders.termQuery("status", 300)).must(QueryBuilders.termQuery("nope", "x"))
        );
    }

    /** A match on an exact-typed field selects the same rows as a term — on the index a match there IS a term query. */
    public void testMatchOnIntegerEqualsTerm() {
        assertSelectsSameRows(QueryBuilders.matchQuery("status", 300));
    }

    public void testMatchOnKeyword() {
        assertSelectsSameRows(QueryBuilders.matchQuery("tags", "t2"));
    }

    /** A match on a field neither source has matches nothing on both — the same leniency as term. */
    public void testMatchOnMissingFieldMatchesNothing() {
        assertSelectsSameRows(QueryBuilders.matchQuery("nope", "x"));
    }

    /** A match_phrase on a keyword field is the whole value — equality — the same rows on both. */
    public void testMatchPhraseOnKeyword() {
        assertSelectsSameRows(QueryBuilders.matchPhraseQuery("tags", "t2"));
    }

    /** multi_match over exact fields is an OR of per-field equality, matching the index's multi_match. */
    public void testMultiMatchOverExactFields() {
        assertSelectsSameRows(QueryBuilders.multiMatchQuery(300, "status", "bytes"));
    }

    public void testMultiMatchSingleField() {
        assertSelectsSameRows(QueryBuilders.multiMatchQuery(300, "status"));
    }

    /** The value matches only the SECOND field (bytes=3000 → one row; status is never 3000) — the OR has teeth. */
    public void testMultiMatchSecondFieldSelects() {
        assertSelectsSameRows(QueryBuilders.multiMatchQuery(3000, "status", "bytes"));
    }

    /**
     * A fieldless multi_match is implicitly lenient on both paths: it searches every field, dropping the ones that
     * cannot hold the value. "t2" matches only the keyword column; the integer/long/date columns drop out.
     */
    public void testFieldlessMultiMatchIsImplicitlyLenient() {
        assertSelectsSameRows(QueryBuilders.multiMatchQuery("t2"));
    }

    /**
     * Fail-closed: a filter mixing a supported {@code term} with an unsupported {@code wildcard} fails the whole query
     * with a 400 naming the construct — the supported clause does not rescue it, and no widened superset is applied.
     */
    public void testUnsupportedConstructFailsTheQuery() {
        QueryBuilder mixed = QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery("status", 300))
            .must(QueryBuilders.wildcardQuery("tags", "t*"));
        Exception e = expectThrows(Exception.class, () -> selectedIds(dataset, mixed));
        Throwable cause = ExceptionsHelper.unwrapCause(e);
        assertThat(cause.getMessage(), containsString("[wildcard]"));
        assertThat("an unsupported construct is a 400, not a 500", ExceptionsHelper.status(cause), equalTo(RestStatus.BAD_REQUEST));
    }
}
