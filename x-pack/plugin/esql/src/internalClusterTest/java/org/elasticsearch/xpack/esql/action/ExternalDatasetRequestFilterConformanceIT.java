/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.cluster.metadata.DatasetFieldMapping;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.unsignedlong.UnsignedLongMapperPlugin;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
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

    @Override
    protected boolean addMockHttpTransport() {
        return false; // real HTTP transport is required for the REST-layer tests
    }

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

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        // unsigned_long is a mapper plugin type, and the index half of the differential needs it to mirror the
        // dataset's declared unsigned_long column.
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(UnsignedLongMapperPlugin.class);
        return plugins;
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

    /** Values land exactly on the bounds the range cases use, so an inclusive and an exclusive bound select different rows. */
    private static double score(int i) {
        return i * 1.5;
    }

    private static String clientIp(int i) {
        return "10.0.0." + i;
    }

    private static long quota(int i) {
        return i * 100L;
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
                    "type=keyword",
                    "score",
                    "type=double",
                    "client_ip",
                    "type=ip",
                    "quota",
                    "type=unsigned_long"
                )
        );
        for (int i = 0; i < ROWS; i++) {
            client().prepareIndex(INDEX)
                .setSource(
                    "id",
                    i,
                    "status",
                    status(i),
                    "tags",
                    tag(i),
                    "bytes",
                    bytes(i),
                    "ts",
                    ts(i),
                    "label",
                    label(i),
                    "score",
                    score(i),
                    "client_ip",
                    clientIp(i),
                    "quota",
                    quota(i)
                )
                .get();
        }
        client().admin().indices().prepareRefresh(INDEX).get();

        // The dataset: identical rows as a strict declared-schema CSV, types matching the index mapping exactly.
        StringBuilder csv = new StringBuilder(
            "id:integer,status:integer,tags:keyword,bytes:long,ts:date,label:keyword,score:double,client_ip:ip,quota:unsigned_long\n"
        );
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
                .append(',')
                .append(score(i))
                .append(',')
                .append(clientIp(i))
                .append(',')
                .append(quota(i))
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
        properties.put("score", new DatasetFieldMapping("double", null));
        properties.put("client_ip", new DatasetFieldMapping("ip", null));
        properties.put("quota", new DatasetFieldMapping("unsigned_long", null));
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

    /**
     * A type with no predecessor or successor cannot have an exclusive bound rewritten onto its neighbour, so the
     * translation carries the inclusivity itself. Each bound here is a stored value, which is what makes the inclusive
     * and exclusive forms select different rows rather than agreeing by accident.
     */
    public void testExclusiveRangeOnDouble() {
        assertSelectsSameRows(QueryBuilders.rangeQuery("score").gt(1.5).lt(9.0));
        assertSelectsSameRows(QueryBuilders.rangeQuery("score").gte(1.5).lt(9.0));
        assertSelectsSameRows(QueryBuilders.rangeQuery("score").gt(1.5).lte(9.0));
        assertSelectsSameRows(QueryBuilders.rangeQuery("score").gte(1.5).lte(9.0));
    }

    /** Same, on keyword — byte order, and the bounds are stored tag values. */
    public void testExclusiveRangeOnKeyword() {
        assertSelectsSameRows(QueryBuilders.rangeQuery("tags").gt("t0").lt("t3"));
        assertSelectsSameRows(QueryBuilders.rangeQuery("tags").gte("t0").lte("t3"));
    }

    /** An ip literal is compared in its encoded form, so "10.0.0.20" must order above "10.0.0.5" on both paths. */
    public void testIpTermAndExclusiveRange() {
        assertSelectsSameRows(QueryBuilders.termQuery("client_ip", "10.0.0.7"));
        assertSelectsSameRows(QueryBuilders.rangeQuery("client_ip").gt("10.0.0.5").lt("10.0.0.20"));
        assertSelectsSameRows(QueryBuilders.rangeQuery("client_ip").gte("10.0.0.5").lte("10.0.0.20"));
    }

    /** An unsigned_long literal arrives as a JSON number and must encode to the field's internal representation. */
    public void testUnsignedLongTermAndExclusiveRange() {
        assertSelectsSameRows(QueryBuilders.termQuery("quota", 700));
        assertSelectsSameRows(QueryBuilders.rangeQuery("quota").gt(500).lt(2000));
        assertSelectsSameRows(QueryBuilders.rangeQuery("quota").gte(500).lte(2000));
    }

    /**
     * unsigned_long is integral, so a value it cannot hold matches nothing and a bound is rounded inward — not
     * truncated toward zero, which would make 700.9 select the rows equal to 700 and 0.5 admit 0.
     */
    public void testUnsignedLongUnmatchableValuesAndInwardBounds() {
        assertSelectsSameRows(QueryBuilders.termQuery("quota", 700.9));
        // parseTerm does not coerce: a whole double, a "700.0" and a padded " 700" are each unmatchable.
        assertSelectsSameRows(QueryBuilders.termQuery("quota", 700.0));
        assertSelectsSameRows(QueryBuilders.termQuery("quota", "700.0"));
        assertSelectsSameRows(QueryBuilders.termQuery("quota", " 700"));
        assertSelectsSameRows(QueryBuilders.termQuery("quota", -5));
        assertSelectsSameRows(QueryBuilders.termsQuery("quota", List.of(700.9, 800)));
        assertSelectsSameRows(QueryBuilders.rangeQuery("quota").gte(0.5));
        assertSelectsSameRows(QueryBuilders.rangeQuery("quota").lte(700.5));
        assertSelectsSameRows(QueryBuilders.rangeQuery("quota").gte(-5));
        assertSelectsSameRows(QueryBuilders.rangeQuery("quota").lte(-5));
    }

    /** A prefix is the wildcard {@code <literal>*}; the literal's own metacharacters must stay literal. */
    public void testPrefix() {
        assertSelectsSameRows(QueryBuilders.prefixQuery("tags", "t"));
        assertSelectsSameRows(QueryBuilders.prefixQuery("tags", "t1"));
        assertSelectsSameRows(QueryBuilders.prefixQuery("label", "A"));
        assertSelectsSameRows(QueryBuilders.prefixQuery("tags", "t*")); // no tag is literally "t*", so nothing matches
    }

    /** mv_like builds its automaton with the same WildcardQuery.toAutomaton call the index makes. */
    public void testWildcard() {
        assertSelectsSameRows(QueryBuilders.wildcardQuery("tags", "t?"));
        assertSelectsSameRows(QueryBuilders.wildcardQuery("tags", "*1"));
        assertSelectsSameRows(QueryBuilders.wildcardQuery("tags", "t*1"));
        assertSelectsSameRows(QueryBuilders.wildcardQuery("label", "?e*"));
    }

    /** Lucene reads an escape of a non-metacharacter as that character; the ES|QL spelling rejects it, so it routes
     *  through mv_rlike instead — and must still select what the index selects. */
    public void testLenientlyEscapedWildcard() {
        assertSelectsSameRows(QueryBuilders.wildcardQuery("tags", "\\t*"));
        assertSelectsSameRows(QueryBuilders.wildcardQuery("tags", "t1\\"));
    }

    /**
     * The escaped character is a literal, so escaping a RegExp metacharacter must not hand mv_rlike that character's
     * RegExp meaning. {@code t\.} selects nothing on the index — no tag is "t." — and a raw {@code .} would make it
     * select every row; {@code t\|1} under-matched the same way, returning FEWER rows than the index.
     */
    public void testWildcardEscapingARegexpMetacharacter() {
        for (String pattern : List.of("t\\.", "t\\|1", "t\\+", "t\\@", "t\\~", "t\\&", "t\\#", "t\\(", "t\\[")) {
            assertSelectsSameRows(QueryBuilders.wildcardQuery("tags", pattern));
        }
        // The positive control: the same characters unescaped keep their RegExp meaning on both sides.
        assertSelectsSameRows(QueryBuilders.wildcardQuery("tags", "t."));
    }

    /** regexp is Lucene RegExp syntax on both sides, with the same RegexpFlag.ALL parse. */
    public void testRegexp() {
        assertSelectsSameRows(QueryBuilders.regexpQuery("tags", "t[01]"));
        assertSelectsSameRows(QueryBuilders.regexpQuery("tags", "t."));
        assertSelectsSameRows(QueryBuilders.regexpQuery("label", "[A-Z].*"));
        assertSelectsSameRows(QueryBuilders.regexpQuery("tags", "x.*")); // matches nothing on either side
    }

    /**
     * The score-only wrappers select what their inner query selects. The differential is the proof: the boosting
     * negative clause here would exclude two thirds of the rows if it were treated as a filter, and dis_max's arms
     * overlap so a union is distinguishable from either arm alone.
     */
    public void testScoreOnlyWrappers() {
        assertSelectsSameRows(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("status", 300)));
        assertSelectsSameRows(QueryBuilders.constantScoreQuery(QueryBuilders.rangeQuery("bytes").gte(5_000).lt(25_000)));
        assertSelectsSameRows(
            QueryBuilders.boostingQuery(QueryBuilders.termQuery("status", 300), QueryBuilders.termQuery("tags", "t1")).negativeBoost(0.1f)
        );
        assertSelectsSameRows(
            QueryBuilders.disMaxQuery().add(QueryBuilders.termQuery("status", 300)).add(QueryBuilders.termQuery("tags", "t1"))
        );
        assertSelectsSameRows(QueryBuilders.disMaxQuery().add(QueryBuilders.termQuery("status", 300)));
    }

    /** A wrapper nests and composes: the inner bool still reports per leaf, so only the fuzzy clause is dropped. */
    public void testWrapperKeepsLeafGranularReporting() {
        QueryBuilder mixed = QueryBuilders.constantScoreQuery(
            QueryBuilders.boolQuery().must(QueryBuilders.termQuery("status", 300)).must(QueryBuilders.fuzzyQuery("tags", "t"))
        );
        assertEquals(selectedIds(dataset, QueryBuilders.termQuery("status", 300)), selectedIds(dataset, mixed));
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
     * An untranslatable clause costs the caller that clause, not the query: a filter mixing a supported {@code term}
     * with an untranslatable {@code fuzzy} in a required must arm answers, selecting exactly what the {@code term}
     * alone selects. That equality is the loosen-only contract — dropping a conjunct can only widen the result, so the
     * dropped clause must not remove a row the term admits, and must not add one either.
     */
    public void testUntranslatableConstructDropsOnlyThatClause() {
        QueryBuilder mixed = QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery("status", 300))
            .must(QueryBuilders.fuzzyQuery("tags", "t"));
        List<Object> withUntranslatable = selectedIds(dataset, mixed);
        List<Object> supportedOnly = selectedIds(dataset, QueryBuilders.termQuery("status", 300));
        assertThat("the fixture must select rows, or the equality below is vacuous", supportedOnly.isEmpty(), equalTo(false));
        assertThat(withUntranslatable, equalTo(supportedOnly));
    }

    /**
     * Non-required should arm with an unsupported construct must NOT fail the query: the applied
     * filter is semantically complete (the must conjunct is the binding constraint; the should is optional).
     */
    public void testNonRequiredShouldUnsupportedDoesNotFailQuery() {
        // bool { must:[term], should:[fuzzy] } — should is non-required because must is present and no msm override.
        QueryBuilder filter = QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery("status", 300))
            .should(QueryBuilders.fuzzyQuery("tags", "t"));
        // Must not throw; rows matching status=300 must be returned.
        List<Object> ids = selectedIds(dataset, filter);
        assertThat("filter on must=300 must return rows", ids.isEmpty(), equalTo(false));
    }

    // ---- REST layer tests: prove the policy and the withdrawn parameter through the HTTP path ----

    /**
     * REST: an untranslatable construct is dropped with a {@code Warning} response header naming it, and the query
     * still answers. No request parameter selects this — it is the only dataset policy there is.
     *
     * <p>The clause is a {@code fuzzy}, deliberately: it is the one construct with no translation and none planned, so
     * this case keeps testing the policy rather than the vocabulary as the translator learns more constructs.
     */
    public void testRestUntranslatableClauseIsDroppedWithWarning() throws IOException {
        Request request = new Request("POST", "/_query");
        request.setJsonEntity(String.format(Locale.ROOT, """
            {
              "query": "FROM %s | KEEP id",
              "filter": { "fuzzy": { "tags": { "value": "t" } } }
            }
            """, dataset));
        Response response = getRestClient().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        List<String> warnings = response.getWarnings();
        assertTrue(
            "expected a warning about the dropped [fuzzy] construct; got: " + warnings,
            warnings.stream().anyMatch(w -> w.contains("[fuzzy]"))
        );
    }

    /**
     * REST: {@code allow_partial_dsl_filter} is no longer a parameter. It never shipped in a release, so it is
     * withdrawn rather than deprecated, and the REST layer rejects it like any other unknown parameter.
     */
    public void testWithdrawnPartialDslFilterParameterIsRejected() throws IOException {
        Request request = new Request("POST", "/_query");
        request.addParameter("allow_partial_dsl_filter", "true");
        request.setJsonEntity(String.format(Locale.ROOT, """
            {
              "query": "FROM %s | KEEP id"
            }
            """, dataset));
        ResponseException e = expectThrows(ResponseException.class, () -> getRestClient().performRequest(request));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(EntityUtils.toString(e.getResponse().getEntity()), containsString("allow_partial_dsl_filter"));
    }
}
