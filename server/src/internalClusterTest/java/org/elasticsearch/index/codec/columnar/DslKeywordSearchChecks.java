/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.columnar;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortMode;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.codec.columnar.BehaviorCheck;
import org.elasticsearch.test.codec.columnar.DuelContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.elasticsearch.test.codec.columnar.DuelAssertions.assertEquals;
import static org.elasticsearch.test.codec.columnar.DuelAssertions.assertSameElements;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;

/**
 * DSL search-path keyword behavior checks: term, terms, prefix, wildcard, regexp, range, simple_query_string,
 * exists, and sort. Query checks compare the set of matching {@code doc_id} values (order and duplicates are
 * not part of the query contract), while the sort check compares the returned hit order and sort values because
 * ordering is the behavior under test. Each check validates the baseline response against the corpus oracle
 * before comparing the contender.
 */
public final class DslKeywordSearchChecks {

    private static final int MAX_HITS = 10_000;
    private static final int MAX_TERMS_PROBED = 5;

    private DslKeywordSearchChecks() {}

    public static List<BehaviorCheck> all() {
        return List.of(
            new TermQueryCheck(),
            new TermsQueryCheck(),
            new PrefixQueryCheck(),
            new WildcardQueryCheck(),
            new RegexpQueryCheck(),
            new RangeQueryCheck(),
            new SimpleQueryStringCheck(),
            new ExistsQueryCheck(),
            new KeywordSortCheck()
        );
    }

    static final class TermQueryCheck implements BehaviorCheck {
        @Override
        public String name() {
            return "dsl_term_query";
        }

        @Override
        public void check(final DuelContext ctx) {
            final List<String> terms = new ArrayList<>(ctx.presentTerms(MAX_TERMS_PROBED));
            terms.add(ctx.absentTerm());
            for (final String term : terms) {
                final QueryBuilder query = QueryBuilders.termQuery(ctx.keywordField(), term);
                final List<Long> expected = sorted(ctx.docIdsContaining(term));
                final List<Long> baseline = sorted(docIds(ctx.client(), ctx.baselineIndex(), query, ctx.docIdField()));
                final List<Long> contender = sorted(docIds(ctx.client(), ctx.contenderIndex(), query, ctx.docIdField()));
                final String context = ctx.failureContext(name() + "[" + term + "]");
                assertSameElements(context + " stage=[baseline-oracle]", expected, baseline);
                assertSameElements(context + " stage=[contender-vs-baseline]", baseline, contender);
            }
        }
    }

    static final class TermsQueryCheck implements BehaviorCheck {
        @Override
        public String name() {
            return "dsl_terms_query";
        }

        @Override
        public void check(final DuelContext ctx) {
            final List<String> present = ctx.presentTerms(MAX_TERMS_PROBED);
            if (present.isEmpty()) {
                return;
            }
            final List<String> terms = new ArrayList<>(present);
            terms.add(ctx.absentTerm());
            final QueryBuilder query = QueryBuilders.termsQuery(ctx.keywordField(), terms);
            final Set<Long> expectedSet = new TreeSet<>();
            for (final String term : present) {
                expectedSet.addAll(ctx.docIdsContaining(term));
            }
            final List<Long> expected = sorted(expectedSet);
            final List<Long> baseline = sorted(docIds(ctx.client(), ctx.baselineIndex(), query, ctx.docIdField()));
            final List<Long> contender = sorted(docIds(ctx.client(), ctx.contenderIndex(), query, ctx.docIdField()));
            final String context = ctx.failureContext(name());
            assertSameElements(context + " stage=[baseline-oracle]", expected, baseline);
            assertSameElements(context + " stage=[contender-vs-baseline]", baseline, contender);
        }
    }

    static final class PrefixQueryCheck implements BehaviorCheck {
        @Override
        public String name() {
            return "dsl_prefix_query";
        }

        @Override
        public void check(final DuelContext ctx) {
            final String prefix = ctx.prefixCharacter();
            assertQueryDocIds(
                ctx,
                name(),
                QueryBuilders.prefixQuery(ctx.keywordField(), prefix),
                ctx.docIdsWithValueMatching(value -> value.startsWith(prefix))
            );
        }
    }

    static final class WildcardQueryCheck implements BehaviorCheck {
        @Override
        public String name() {
            return "dsl_wildcard_query";
        }

        @Override
        public void check(final DuelContext ctx) {
            final String prefix = ctx.prefixCharacter();
            assertQueryDocIds(
                ctx,
                name(),
                QueryBuilders.wildcardQuery(ctx.keywordField(), prefix + "*"),
                ctx.docIdsWithValueMatching(value -> value.startsWith(prefix))
            );
        }
    }

    static final class RegexpQueryCheck implements BehaviorCheck {
        @Override
        public String name() {
            return "dsl_regexp_query";
        }

        @Override
        public void check(final DuelContext ctx) {
            final String prefix = ctx.prefixCharacter();
            assertQueryDocIds(
                ctx,
                name(),
                QueryBuilders.regexpQuery(ctx.keywordField(), prefix + ".*"),
                ctx.docIdsWithValueMatching(value -> value.startsWith(prefix))
            );
        }
    }

    static final class RangeQueryCheck implements BehaviorCheck {
        @Override
        public String name() {
            return "dsl_range_query";
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
            assertQueryDocIds(
                ctx,
                name(),
                QueryBuilders.rangeQuery(ctx.keywordField()).gte(lower).lt(upper),
                ctx.docIdsWithValueMatching(value -> lower.compareTo(value) <= 0 && value.compareTo(upper) < 0)
            );
        }
    }

    static final class SimpleQueryStringCheck implements BehaviorCheck {
        @Override
        public String name() {
            return "dsl_simple_query_string";
        }

        @Override
        public void check(final DuelContext ctx) {
            final String term = ctx.literalTerm();
            assertQueryDocIds(
                ctx,
                name(),
                QueryBuilders.simpleQueryStringQuery(term).field(ctx.keywordField()),
                ctx.docIdsContaining(term)
            );
        }
    }

    static final class ExistsQueryCheck implements BehaviorCheck {
        @Override
        public String name() {
            return "dsl_exists_query";
        }

        @Override
        public void check(final DuelContext ctx) {
            final QueryBuilder query = QueryBuilders.existsQuery(ctx.keywordField());
            final List<Long> expected = sorted(ctx.docIdsWithAnyValue());
            final List<Long> baseline = sorted(docIds(ctx.client(), ctx.baselineIndex(), query, ctx.docIdField()));
            final List<Long> contender = sorted(docIds(ctx.client(), ctx.contenderIndex(), query, ctx.docIdField()));
            final String context = ctx.failureContext(name());
            assertSameElements(context + " stage=[baseline-oracle]", expected, baseline);
            assertSameElements(context + " stage=[contender-vs-baseline]", baseline, contender);
        }
    }

    static final class KeywordSortCheck implements BehaviorCheck {
        @Override
        public String name() {
            return "dsl_keyword_sort";
        }

        @Override
        public void check(final DuelContext ctx) {
            assertSort(ctx, SortOrder.ASC, SortMode.MIN);
            assertSort(ctx, SortOrder.DESC, SortMode.MAX);
        }

        private void assertSort(final DuelContext ctx, final SortOrder order, final SortMode mode) {
            final List<String> baseline = sortedHits(ctx.client(), ctx.baselineIndex(), ctx.keywordField(), ctx.docIdField(), order, mode);
            final List<String> contender = sortedHits(
                ctx.client(),
                ctx.contenderIndex(),
                ctx.keywordField(),
                ctx.docIdField(),
                order,
                mode
            );
            final String context = ctx.failureContext(name() + "[" + order + "," + mode + "]");
            if (baseline.size() != ctx.docs().size()) {
                throw new AssertionError(
                    context + " stage=[baseline-completeness] expected " + ctx.docs().size() + " hits but got " + baseline.size()
                );
            }
            assertEquals(context + " stage=[contender-vs-baseline]", baseline, contender);
        }
    }

    private static void assertQueryDocIds(
        final DuelContext ctx,
        final String checkName,
        final QueryBuilder query,
        final Set<Long> expected
    ) {
        final List<Long> expectedIds = sorted(expected);
        final List<Long> baseline = sorted(docIds(ctx.client(), ctx.baselineIndex(), query, ctx.docIdField()));
        final List<Long> contender = sorted(docIds(ctx.client(), ctx.contenderIndex(), query, ctx.docIdField()));
        final String context = ctx.failureContext(checkName);
        assertSameElements(context + " stage=[baseline-oracle]", expectedIds, baseline);
        assertSameElements(context + " stage=[contender-vs-baseline]", baseline, contender);
    }

    private static Set<Long> docIds(final Client client, final String index, final QueryBuilder query, final String docIdField) {
        final Set<Long> ids = new TreeSet<>();
        assertResponse(
            client.prepareSearch(index).setQuery(query).setSize(MAX_HITS).setTrackTotalHits(true).addDocValueField(docIdField),
            response -> {
                for (final SearchHit hit : response.getHits().getHits()) {
                    ids.add(((Number) hit.field(docIdField).getValue()).longValue());
                }
            }
        );
        return ids;
    }

    private static List<String> sortedHits(
        final Client client,
        final String index,
        final String keywordField,
        final String docIdField,
        final SortOrder order,
        final SortMode mode
    ) {
        final List<String> ordered = new ArrayList<>();
        assertResponse(
            client.prepareSearch(index)
                .setQuery(QueryBuilders.matchAllQuery())
                .setSize(MAX_HITS)
                .addSort(SortBuilders.fieldSort(keywordField).order(order).sortMode(mode))
                .addSort(SortBuilders.fieldSort(docIdField).order(SortOrder.ASC)),
            response -> {
                for (final SearchHit hit : response.getHits().getHits()) {
                    final Object[] sortValues = hit.getSortValues();
                    ordered.add(sortValues[1] + "|" + sortValues[0]);
                }
            }
        );
        return ordered;
    }

    private static List<Long> sorted(final Set<Long> ids) {
        return List.copyOf(new TreeSet<>(ids));
    }
}
