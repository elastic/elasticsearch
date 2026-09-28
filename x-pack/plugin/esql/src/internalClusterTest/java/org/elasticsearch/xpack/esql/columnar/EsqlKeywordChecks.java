/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.columnar;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.codec.columnar.BehaviorCheck;
import org.elasticsearch.test.codec.columnar.DuelContext;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.elasticsearch.test.codec.columnar.DuelAssertions.assertCount;
import static org.elasticsearch.test.codec.columnar.DuelAssertions.assertEqualBuckets;
import static org.elasticsearch.test.codec.columnar.DuelAssertions.assertEquals;
import static org.elasticsearch.test.codec.columnar.DuelAssertions.assertSameElements;
import static org.elasticsearch.test.codec.columnar.DuelAssertions.assertSameKeys;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.paramAsConstant;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;

/**
 * ES|QL keyword behavior checks. Each check runs the same query against the baseline and a contender index and
 * compares the result rows, never warnings, because ES|QL warning emission for multi-valued fields varies with
 * filter pushdown and is not part of the result contract. Only multivalue-safe constructs are used, and
 * execution pins {@link QueryPragmas#EMPTY} so both indices run under identical strategy. Query checks compare
 * doc-id sets; sort, aggregation, and retrieval checks compare the ordered or keyed values under test.
 */
public final class EsqlKeywordChecks {

    private static final int MAX_TERMS_PROBED = 5;

    private EsqlKeywordChecks() {}

    public static List<BehaviorCheck> all() {
        return List.of(
            new TermMembershipCheck(),
            new StartsWithCheck(),
            new LikeCheck(),
            new RlikeCheck(),
            new LexRangeCheck(),
            new ExistsCheck(),
            new IsNullCheck(),
            new SortCheck(),
            new StatsByKeywordCheck(),
            new CountDistinctCheck(),
            new ValueCountCheck(),
            new ValuesCheck(),
            new RetrievalCheck()
        );
    }

    static final class TermMembershipCheck implements BehaviorCheck {
        @Override
        public String name() {
            return "esql_term_membership";
        }

        @Override
        public void check(final DuelContext ctx) {
            final List<String> terms = new ArrayList<>(ctx.presentTerms(MAX_TERMS_PROBED));
            terms.add(ctx.absentTerm());
            final String query = "FROM "
                + "%s"
                + " | MV_EXPAND "
                + ctx.keywordField()
                + " | WHERE "
                + ctx.keywordField()
                + " == ?term | KEEP "
                + ctx.docIdField()
                + " | SORT "
                + ctx.docIdField();
            for (final String term : terms) {
                final QueryParams params = new QueryParams(List.of(paramAsConstant("term", term)));
                final List<Long> expected = sorted(ctx.docIdsContaining(term));
                final List<Long> baseline = docIds(runEsql(ctx.client(), Strings.format(query, ctx.baselineIndex()), params));
                final List<Long> contender = docIds(runEsql(ctx.client(), Strings.format(query, ctx.contenderIndex()), params));
                final String context = ctx.failureContext(name() + "[" + term + "]");
                assertSameElements(context + " stage=[baseline-oracle]", expected, baseline);
                assertSameElements(context + " stage=[contender-vs-baseline]", baseline, contender);
            }
        }
    }

    static final class ExistsCheck implements BehaviorCheck {
        @Override
        public String name() {
            return "esql_exists";
        }

        @Override
        public void check(final DuelContext ctx) {
            final String query = "FROM %s | WHERE "
                + ctx.keywordField()
                + " IS NOT NULL | KEEP "
                + ctx.docIdField()
                + " | SORT "
                + ctx.docIdField();
            final List<Long> expected = sorted(ctx.docIdsWithAnyValue());
            final List<Long> baseline = docIds(runEsql(ctx.client(), Strings.format(query, ctx.baselineIndex())));
            final List<Long> contender = docIds(runEsql(ctx.client(), Strings.format(query, ctx.contenderIndex())));
            final String context = ctx.failureContext(name());
            assertSameElements(context + " stage=[baseline-oracle]", expected, baseline);
            assertSameElements(context + " stage=[contender-vs-baseline]", baseline, contender);
        }
    }

    static final class StartsWithCheck implements BehaviorCheck {
        @Override
        public String name() {
            return "esql_starts_with";
        }

        @Override
        public void check(final DuelContext ctx) {
            final String prefix = ctx.prefixCharacter();
            final String query = "FROM %s | MV_EXPAND "
                + ctx.keywordField()
                + " | WHERE STARTS_WITH("
                + ctx.keywordField()
                + ", ?prefix) | KEEP "
                + ctx.docIdField()
                + " | SORT "
                + ctx.docIdField();
            final QueryParams params = new QueryParams(List.of(paramAsConstant("prefix", prefix)));
            assertDocIds(ctx, name(), query, params, ctx.docIdsWithValueMatching(value -> value.startsWith(prefix)));
        }
    }

    static final class LikeCheck implements BehaviorCheck {
        @Override
        public String name() {
            return "esql_like";
        }

        @Override
        public void check(final DuelContext ctx) {
            final String prefix = ctx.prefixCharacter();
            final String query = "FROM %s | MV_EXPAND "
                + ctx.keywordField()
                + " | WHERE "
                + ctx.keywordField()
                + " LIKE \""
                + prefix
                + "*\" | KEEP "
                + ctx.docIdField()
                + " | SORT "
                + ctx.docIdField();
            assertDocIds(ctx, name(), query, null, ctx.docIdsWithValueMatching(value -> value.startsWith(prefix)));
        }
    }

    static final class RlikeCheck implements BehaviorCheck {
        @Override
        public String name() {
            return "esql_rlike";
        }

        @Override
        public void check(final DuelContext ctx) {
            final String prefix = ctx.prefixCharacter();
            final String query = "FROM %s | MV_EXPAND "
                + ctx.keywordField()
                + " | WHERE "
                + ctx.keywordField()
                + " RLIKE \""
                + prefix
                + ".*\" | KEEP "
                + ctx.docIdField()
                + " | SORT "
                + ctx.docIdField();
            assertDocIds(ctx, name(), query, null, ctx.docIdsWithValueMatching(value -> value.startsWith(prefix)));
        }
    }

    static final class LexRangeCheck implements BehaviorCheck {
        @Override
        public String name() {
            return "esql_lex_range";
        }

        @Override
        public void check(final DuelContext ctx) {
            final List<String> ascii = ctx.asciiValues();
            if (ascii.size() < 2) {
                // No two ASCII values to bound an unambiguous lexicographic range in this scenario.
                return;
            }
            final String lower = ascii.get(0);
            final String upper = ascii.get(ascii.size() - 1);
            final String query = "FROM %s | MV_EXPAND "
                + ctx.keywordField()
                + " | WHERE "
                + ctx.keywordField()
                + " >= ?lower AND "
                + ctx.keywordField()
                + " < ?upper | KEEP "
                + ctx.docIdField()
                + " | SORT "
                + ctx.docIdField();
            final QueryParams params = new QueryParams(List.of(paramAsConstant("lower", lower), paramAsConstant("upper", upper)));
            assertDocIds(
                ctx,
                name(),
                query,
                params,
                ctx.docIdsWithValueMatching(value -> lower.compareTo(value) <= 0 && value.compareTo(upper) < 0)
            );
        }
    }

    static final class IsNullCheck implements BehaviorCheck {
        @Override
        public String name() {
            return "esql_is_null";
        }

        @Override
        public void check(final DuelContext ctx) {
            final String query = "FROM %s | WHERE "
                + ctx.keywordField()
                + " IS NULL | KEEP "
                + ctx.docIdField()
                + " | SORT "
                + ctx.docIdField();
            assertDocIds(ctx, name(), query, null, ctx.docIdsWithoutValue());
        }
    }

    static final class SortCheck implements BehaviorCheck {
        @Override
        public String name() {
            return "esql_sort";
        }

        @Override
        public void check(final DuelContext ctx) {
            assertSort(ctx, "MV_MIN", "ASC");
            assertSort(ctx, "MV_MAX", "DESC");
        }

        private void assertSort(final DuelContext ctx, final String reducer, final String order) {
            final String query = "FROM %s | EVAL k = "
                + reducer
                + "("
                + ctx.keywordField()
                + ") | SORT k "
                + order
                + ", "
                + ctx.docIdField()
                + " ASC | KEEP "
                + ctx.docIdField()
                + ", k";
            final List<String> baseline = orderedRows(runEsql(ctx.client(), Strings.format(query, ctx.baselineIndex())));
            final List<String> contender = orderedRows(runEsql(ctx.client(), Strings.format(query, ctx.contenderIndex())));
            final String context = ctx.failureContext(name() + "[" + reducer + "," + order + "]");
            if (baseline.size() != ctx.docs().size()) {
                throw new AssertionError(
                    context + " stage=[baseline-completeness] expected " + ctx.docs().size() + " rows but got " + baseline.size()
                );
            }
            assertEquals(context + " stage=[contender-vs-baseline]", baseline, contender);
        }
    }

    static final class StatsByKeywordCheck implements BehaviorCheck {
        @Override
        public String name() {
            return "esql_stats_by_keyword";
        }

        @Override
        public void check(final DuelContext ctx) {
            // Exclude the null group so the bucket keys are the distinct values, matching the DSL terms
            // aggregation contract; the value-less documents are covered by the exists check.
            final String query = "FROM %s | WHERE "
                + ctx.keywordField()
                + " IS NOT NULL | STATS c = COUNT(*) BY "
                + ctx.keywordField()
                + " | KEEP "
                + ctx.keywordField()
                + ", c | SORT "
                + ctx.keywordField();
            final Map<String, Long> baseline = keyedCounts(runEsql(ctx.client(), Strings.format(query, ctx.baselineIndex())));
            final Map<String, Long> contender = keyedCounts(runEsql(ctx.client(), Strings.format(query, ctx.contenderIndex())));
            final String context = ctx.failureContext(name());
            final Set<String> keys = new TreeSet<>(baseline.keySet());
            final Set<String> distinct = new TreeSet<>(ctx.distinctValues());
            if (keys.equals(distinct) == false) {
                throw new AssertionError(context + " stage=[baseline-sanity] keys=" + keys + " distinct=" + distinct);
            }
            assertEqualBuckets(context + " stage=[contender-vs-baseline]", baseline, contender);
        }
    }

    static final class CountDistinctCheck implements BehaviorCheck {
        @Override
        public String name() {
            return "esql_count_distinct";
        }

        @Override
        public void check(final DuelContext ctx) {
            final String query = "FROM %s | STATS d = COUNT_DISTINCT(" + ctx.keywordField() + ")";
            final long baseline = scalar(runEsql(ctx.client(), Strings.format(query, ctx.baselineIndex())));
            final long contender = scalar(runEsql(ctx.client(), Strings.format(query, ctx.contenderIndex())));
            final String context = ctx.failureContext(name());
            // COUNT_DISTINCT is an approximate aggregation, so the contender is compared to the baseline, which
            // estimates the same values identically, rather than to an exact distinct-value oracle.
            if (contender != baseline) {
                throw new AssertionError(context + " stage=[contender-vs-baseline] expected=" + baseline + " actual=" + contender);
            }
        }
    }

    // COUNT over a keyword counts values, including intra-document duplicates, which both strict columnar layouts
    // keep, so it proves the baseline and ColumNAR agree on duplicate handling.
    static final class ValueCountCheck implements BehaviorCheck {
        @Override
        public String name() {
            return "esql_value_count";
        }

        @Override
        public void check(final DuelContext ctx) {
            final String query = "FROM %s | STATS n = COUNT(" + ctx.keywordField() + ")";
            final long baseline = scalar(runEsql(ctx.client(), Strings.format(query, ctx.baselineIndex())));
            final long contender = scalar(runEsql(ctx.client(), Strings.format(query, ctx.contenderIndex())));
            final String context = ctx.failureContext(name());
            assertCount(context + " stage=[baseline-oracle]", ctx.expectedValueCount(), baseline);
            assertCount(context + " stage=[contender-vs-baseline]", baseline, contender);
        }
    }

    static final class ValuesCheck implements BehaviorCheck {
        @Override
        public String name() {
            return "esql_values";
        }

        @Override
        public void check(final DuelContext ctx) {
            final String query = "FROM %s | STATS v = VALUES(" + ctx.keywordField() + ")";
            final List<String> expected = ctx.distinctValues();
            final List<String> baseline = flatten(single(runEsql(ctx.client(), Strings.format(query, ctx.baselineIndex()))));
            final List<String> contender = flatten(single(runEsql(ctx.client(), Strings.format(query, ctx.contenderIndex()))));
            final String context = ctx.failureContext(name());
            assertSameElements(context + " stage=[baseline-oracle]", expected, baseline);
            assertSameElements(context + " stage=[contender-vs-baseline]", baseline, contender);
        }
    }

    static final class RetrievalCheck implements BehaviorCheck {
        @Override
        public String name() {
            return "esql_retrieval";
        }

        @Override
        public void check(final DuelContext ctx) {
            final String query = "FROM %s | KEEP " + ctx.docIdField() + ", " + ctx.keywordField() + " | SORT " + ctx.docIdField();
            final Map<Long, List<String>> expected = ctx.perDocSortedDistinct();
            final Map<Long, List<String>> baseline = perDoc(runEsql(ctx.client(), Strings.format(query, ctx.baselineIndex())));
            final Map<Long, List<String>> contender = perDoc(runEsql(ctx.client(), Strings.format(query, ctx.contenderIndex())));
            assertSameKeys(ctx.failureContext(name()), expected.keySet(), baseline.keySet(), contender.keySet());
            for (final Map.Entry<Long, List<String>> entry : expected.entrySet()) {
                final Long docId = entry.getKey();
                final String context = ctx.failureContext(name() + "[doc=" + docId + "]");
                assertSameElements(context + " stage=[baseline-oracle]", entry.getValue(), baseline.getOrDefault(docId, List.of()));
                assertSameElements(
                    context + " stage=[contender-vs-baseline]",
                    baseline.getOrDefault(docId, List.of()),
                    contender.getOrDefault(docId, List.of())
                );
            }
        }
    }

    private static void assertDocIds(
        final DuelContext ctx,
        final String checkName,
        final String query,
        final QueryParams params,
        final Set<Long> expected
    ) {
        final List<Long> expectedIds = sorted(expected);
        final List<Long> baseline = docIds(runEsql(ctx.client(), Strings.format(query, ctx.baselineIndex()), params));
        final List<Long> contender = docIds(runEsql(ctx.client(), Strings.format(query, ctx.contenderIndex()), params));
        final String context = ctx.failureContext(checkName);
        assertSameElements(context + " stage=[baseline-oracle]", expectedIds, baseline);
        assertSameElements(context + " stage=[contender-vs-baseline]", baseline, contender);
    }

    private static List<List<Object>> runEsql(final Client client, final String query) {
        return runEsql(client, query, null);
    }

    private static List<List<Object>> runEsql(final Client client, final String query, final QueryParams params) {
        final EsqlQueryRequest request = syncEsqlQueryRequest(query);
        request.pragmas(QueryPragmas.EMPTY);
        if (params != null) {
            request.params(params);
        }
        try (EsqlQueryResponse response = client.execute(EsqlQueryAction.INSTANCE, request).actionGet()) {
            return getValuesList(response);
        }
    }

    private static List<Long> docIds(final List<List<Object>> rows) {
        final Set<Long> ids = new TreeSet<>();
        for (final List<Object> row : rows) {
            ids.add(((Number) row.get(0)).longValue());
        }
        return List.copyOf(ids);
    }

    private static List<String> orderedRows(final List<List<Object>> rows) {
        final List<String> ordered = new ArrayList<>(rows.size());
        for (final List<Object> row : rows) {
            ordered.add(row.get(0) + "|" + String.valueOf(row.get(1)));
        }
        return ordered;
    }

    private static Map<String, Long> keyedCounts(final List<List<Object>> rows) {
        final Map<String, Long> counts = new TreeMap<>();
        for (final List<Object> row : rows) {
            counts.put(String.valueOf(row.get(0)), ((Number) row.get(1)).longValue());
        }
        return counts;
    }

    private static long scalar(final List<List<Object>> rows) {
        return ((Number) rows.get(0).get(0)).longValue();
    }

    private static Object single(final List<List<Object>> rows) {
        return rows.get(0).get(0);
    }

    private static Map<Long, List<String>> perDoc(final List<List<Object>> rows) {
        final Map<Long, List<String>> byDoc = new TreeMap<>();
        for (final List<Object> row : rows) {
            byDoc.put(((Number) row.get(0)).longValue(), flatten(row.get(1)));
        }
        return byDoc;
    }

    private static List<String> flatten(final Object value) {
        if (value == null) {
            return List.of();
        }
        if (value instanceof List<?> list) {
            final List<String> values = new ArrayList<>(list.size());
            for (final Object element : list) {
                values.add(String.valueOf(element));
            }
            return values;
        }
        return List.of(String.valueOf(value));
    }

    private static List<Long> sorted(final Set<Long> ids) {
        return List.copyOf(new TreeSet<>(ids));
    }
}
