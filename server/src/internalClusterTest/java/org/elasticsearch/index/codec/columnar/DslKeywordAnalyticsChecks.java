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
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Cardinality;
import org.elasticsearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ValueCount;
import org.elasticsearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.codec.columnar.BehaviorCheck;
import org.elasticsearch.test.codec.columnar.DuelContext;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.codec.columnar.DuelAssertions.assertCount;
import static org.elasticsearch.test.codec.columnar.DuelAssertions.assertEqualBuckets;
import static org.elasticsearch.test.codec.columnar.DuelAssertions.assertEquals;
import static org.elasticsearch.test.codec.columnar.DuelAssertions.assertSameElements;
import static org.elasticsearch.test.codec.columnar.DuelAssertions.assertSameKeys;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;

/**
 * DSL analytics-path keyword behavior checks: terms, cardinality, value_count, and composite aggregations,
 * plus {@code docvalue_fields} and {@code _source} retrieval. Aggregation checks compare bucket key and count
 * pairs ignoring bucket order; {@code docvalue_fields} compares each document's distinct values. The
 * {@code _source} round-trip compares each document's values in source order keeping duplicates, so it asserts
 * the array order and multiplicity the columnar {@code ArrayOrderInlineNull} layout reconstructs, which the
 * order- and multiplicity-insensitive checks do not cover. The value-multiplicity checks (value_count and
 * composite) count every value occurrence including intra-document duplicates, which both strict columnar
 * layouts keep, so they prove the baseline and ColumNAR agree on duplicate handling. Cardinality is an
 * approximate aggregation, so it is compared contender to baseline rather than to an exact oracle; the remaining
 * checks validate the baseline against the corpus oracle and then compare the contender to the baseline.
 */
public final class DslKeywordAnalyticsChecks {

    private static final int MAX_BUCKETS = 10_000;
    private static final int COMPOSITE_PAGE_SIZE = 3;
    private static final int MAX_HITS = 10_000;

    private DslKeywordAnalyticsChecks() {}

    public static List<BehaviorCheck> all() {
        return List.of(
            new TermsAggregationCheck(),
            new CardinalityCheck(),
            new ValueCountCheck(),
            new CompositeAggregationCheck(),
            new DocValueFieldsCheck(),
            new SourceRoundTripCheck()
        );
    }

    static final class TermsAggregationCheck implements BehaviorCheck {
        @Override
        public String name() {
            return "dsl_terms_aggregation";
        }

        @Override
        public void check(final DuelContext ctx) {
            final Map<String, Long> expected = ctx.valueDocCounts();
            final Map<String, Long> baseline = termsBuckets(ctx.client(), ctx.baselineIndex(), ctx.keywordField());
            final Map<String, Long> contender = termsBuckets(ctx.client(), ctx.contenderIndex(), ctx.keywordField());
            final String context = ctx.failureContext(name());
            assertEqualBuckets(context + " stage=[baseline-oracle]", expected, baseline);
            assertEqualBuckets(context + " stage=[contender-vs-baseline]", baseline, contender);
        }
    }

    static final class CardinalityCheck implements BehaviorCheck {
        @Override
        public String name() {
            return "dsl_cardinality";
        }

        @Override
        public void check(final DuelContext ctx) {
            final long baseline = cardinality(ctx.client(), ctx.baselineIndex(), ctx.keywordField());
            final long contender = cardinality(ctx.client(), ctx.contenderIndex(), ctx.keywordField());
            // cardinality is an approximate aggregation, so the contender is compared to the baseline, which
            // estimates the same values identically, rather than to an exact distinct-value oracle.
            assertCount(ctx.failureContext(name()) + " stage=[contender-vs-baseline]", baseline, contender);
        }
    }

    static final class ValueCountCheck implements BehaviorCheck {
        @Override
        public String name() {
            return "dsl_value_count";
        }

        @Override
        public void check(final DuelContext ctx) {
            final long baseline = valueCount(ctx.client(), ctx.baselineIndex(), ctx.keywordField());
            final long contender = valueCount(ctx.client(), ctx.contenderIndex(), ctx.keywordField());
            final String context = ctx.failureContext(name());
            assertCount(context + " stage=[baseline-oracle]", ctx.expectedValueCount(), baseline);
            assertCount(context + " stage=[contender-vs-baseline]", baseline, contender);
        }
    }

    static final class CompositeAggregationCheck implements BehaviorCheck {
        @Override
        public String name() {
            return "dsl_composite_aggregation";
        }

        @Override
        public void check(final DuelContext ctx) {
            final Map<String, Long> baseline = compositeBuckets(ctx.client(), ctx.baselineIndex(), ctx.keywordField());
            final Map<String, Long> contender = compositeBuckets(ctx.client(), ctx.contenderIndex(), ctx.keywordField());
            final String context = ctx.failureContext(name());
            assertEqualBuckets(context + " stage=[baseline-oracle]", ctx.expectedValueBuckets(), baseline);
            assertEqualBuckets(context + " stage=[contender-vs-baseline]", baseline, contender);
        }
    }

    static final class DocValueFieldsCheck implements BehaviorCheck {
        @Override
        public String name() {
            return "dsl_docvalue_fields";
        }

        @Override
        public void check(final DuelContext ctx) {
            final Map<Long, List<String>> expected = ctx.perDocSortedDistinct();
            final Map<Long, List<String>> baseline = docValues(ctx.client(), ctx.baselineIndex(), ctx.keywordField(), ctx.docIdField());
            final Map<Long, List<String>> contender = docValues(ctx.client(), ctx.contenderIndex(), ctx.keywordField(), ctx.docIdField());
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

    static final class SourceRoundTripCheck implements BehaviorCheck {
        @Override
        public String name() {
            return "dsl_source_round_trip";
        }

        @Override
        public void check(final DuelContext ctx) {
            final Map<Long, List<String>> expected = ctx.perDocOrderedValues();
            final Map<Long, List<String>> baseline = sourceValues(ctx.client(), ctx.baselineIndex(), ctx.keywordField(), ctx.docIdField());
            final Map<Long, List<String>> contender = sourceValues(
                ctx.client(),
                ctx.contenderIndex(),
                ctx.keywordField(),
                ctx.docIdField()
            );
            assertSameKeys(ctx.failureContext(name()), expected.keySet(), baseline.keySet(), contender.keySet());
            for (final Map.Entry<Long, List<String>> entry : expected.entrySet()) {
                final Long docId = entry.getKey();
                final String context = ctx.failureContext(name() + "[doc=" + docId + "]");
                assertEquals(context + " stage=[baseline-oracle]", entry.getValue(), baseline.getOrDefault(docId, List.of()));
                assertEquals(
                    context + " stage=[contender-vs-baseline]",
                    baseline.getOrDefault(docId, List.of()),
                    contender.getOrDefault(docId, List.of())
                );
            }
        }
    }

    private static Map<String, Long> termsBuckets(final Client client, final String index, final String field) {
        final Map<String, Long> buckets = new LinkedHashMap<>();
        assertResponse(
            client.prepareSearch(index).setSize(0).addAggregation(new TermsAggregationBuilder("terms").field(field).size(MAX_BUCKETS)),
            response -> {
                final Terms terms = response.getAggregations().get("terms");
                for (final Terms.Bucket bucket : terms.getBuckets()) {
                    buckets.put(bucket.getKeyAsString(), bucket.getDocCount());
                }
            }
        );
        return buckets;
    }

    private static long cardinality(final Client client, final String index, final String field) {
        final long[] value = new long[1];
        assertResponse(
            client.prepareSearch(index).setSize(0).addAggregation(new CardinalityAggregationBuilder("card").field(field)),
            response -> {
                final Cardinality cardinality = response.getAggregations().get("card");
                value[0] = cardinality.getValue();
            }
        );
        return value[0];
    }

    private static long valueCount(final Client client, final String index, final String field) {
        final long[] value = new long[1];
        assertResponse(
            client.prepareSearch(index).setSize(0).addAggregation(new ValueCountAggregationBuilder("vc").field(field)),
            response -> {
                final ValueCount valueCount = response.getAggregations().get("vc");
                value[0] = valueCount.getValue();
            }
        );
        return value[0];
    }

    private static Map<String, Long> compositeBuckets(final Client client, final String index, final String field) {
        final Map<String, Long> buckets = new TreeMap<>();
        Map<String, Object> after = null;
        while (true) {
            final CompositeAggregationBuilder aggregation = new CompositeAggregationBuilder(
                "comp",
                List.of(new TermsValuesSourceBuilder(field).field(field))
            ).size(COMPOSITE_PAGE_SIZE);
            if (after != null) {
                aggregation.aggregateAfter(after);
            }
            final AtomicReference<Map<String, Object>> nextAfter = new AtomicReference<>();
            final int[] returned = new int[1];
            assertResponse(client.prepareSearch(index).setSize(0).addAggregation(aggregation), response -> {
                final CompositeAggregation composite = response.getAggregations().get("comp");
                for (final CompositeAggregation.Bucket bucket : composite.getBuckets()) {
                    buckets.merge(String.valueOf(bucket.getKey().get(field)), bucket.getDocCount(), Long::sum);
                }
                returned[0] = composite.getBuckets().size();
                nextAfter.set(composite.afterKey());
            });
            if (returned[0] < COMPOSITE_PAGE_SIZE || nextAfter.get() == null) {
                break;
            }
            after = nextAfter.get();
        }
        return buckets;
    }

    private static Map<Long, List<String>> docValues(
        final Client client,
        final String index,
        final String keywordField,
        final String docIdField
    ) {
        final Map<Long, List<String>> byDoc = new TreeMap<>();
        assertResponse(
            client.prepareSearch(index)
                .setQuery(QueryBuilders.matchAllQuery())
                .setSize(MAX_HITS)
                .addDocValueField(keywordField)
                .addDocValueField(docIdField)
                .addSort(SortBuilders.fieldSort(docIdField).order(SortOrder.ASC)),
            response -> {
                for (final SearchHit hit : response.getHits().getHits()) {
                    final long docId = ((Number) hit.field(docIdField).getValue()).longValue();
                    final List<String> values = new ArrayList<>();
                    if (hit.field(keywordField) != null) {
                        for (final Object value : hit.field(keywordField).getValues()) {
                            values.add(String.valueOf(value));
                        }
                    }
                    byDoc.put(docId, values);
                }
            }
        );
        return byDoc;
    }

    private static Map<Long, List<String>> sourceValues(
        final Client client,
        final String index,
        final String keywordField,
        final String docIdField
    ) {
        final Map<Long, List<String>> byDoc = new TreeMap<>();
        assertResponse(
            client.prepareSearch(index)
                .setQuery(QueryBuilders.matchAllQuery())
                .setSize(MAX_HITS)
                .addDocValueField(docIdField)
                .addSort(SortBuilders.fieldSort(docIdField).order(SortOrder.ASC)),
            response -> {
                for (final SearchHit hit : response.getHits().getHits()) {
                    final long docId = ((Number) hit.field(docIdField).getValue()).longValue();
                    byDoc.put(docId, sourceKeyword(hit, keywordField));
                }
            }
        );
        return byDoc;
    }

    private static List<String> sourceKeyword(final SearchHit hit, final String keywordField) {
        final Map<String, Object> source = hit.getSourceAsMap();
        final Object raw = source == null ? null : source.get(keywordField);
        if (raw == null) {
            return List.of();
        }
        if (raw instanceof List<?> list) {
            final List<String> values = new ArrayList<>(list.size());
            for (final Object element : list) {
                values.add(element == null ? null : String.valueOf(element));
            }
            return values;
        }
        return List.of(String.valueOf(raw));
    }
}
