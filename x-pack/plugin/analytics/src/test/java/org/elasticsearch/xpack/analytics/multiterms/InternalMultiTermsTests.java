/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.multiterms;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.search.DocValueFormat.UNSIGNED_LONG_SHIFTED;
import static org.elasticsearch.xpack.analytics.multiterms.InternalMultiTerms.KeyConverter.DOUBLE;
import static org.elasticsearch.xpack.analytics.multiterms.InternalMultiTerms.KeyConverter.LONG;
import static org.elasticsearch.xpack.analytics.multiterms.InternalMultiTerms.KeyConverter.UNSIGNED_LONG;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;

public class InternalMultiTermsTests extends InternalAggregationTestCase<InternalMultiTerms> {

    @Override
    protected SearchPlugin registerPlugin() {
        return new AnalyticsPlugin(Settings.EMPTY);
    }

    static BucketOrder randomBucketOrder() {
        return randomBucketOrder(true);
    }

    private static BucketOrder randomBucketOrder(boolean includeCompound) {
        switch (randomInt(includeCompound ? 4 : 3)) {
            case 0:
                return BucketOrder.key(randomBoolean());
            case 1:
                return BucketOrder.count(randomBoolean());
            case 2:
                return BucketOrder.aggregation(randomAlphaOfLengthBetween(3, 20), randomBoolean());
            case 3:
                return BucketOrder.aggregation(randomAlphaOfLengthBetween(3, 20), randomAlphaOfLengthBetween(3, 20), randomBoolean());
            case 4:
                List<BucketOrder> orders = new ArrayList<>();
                int numOrders = randomIntBetween(2, 5);
                for (int i = 0; i < numOrders; i++) {
                    orders.add(randomBucketOrder(false));
                }
                return BucketOrder.compound(orders);
            default:
                fail();
        }
        return null;
    }

    private List<DocValueFormat> randomFormats(int size) {
        return randomList(size, size, InternalAggregationTestCase::randomNumericDocValueFormat);
    }

    private List<InternalMultiTerms.KeyConverter> randomKeyConverters(int size) {
        return randomList(size, size, () -> randomFrom(InternalMultiTerms.KeyConverter.values()));
    }

    private List<InternalMultiTerms.Bucket> randomBuckets() {
        return Collections.emptyList();
    }

    @Override
    protected InternalMultiTerms createTestInstance(String name, Map<String, Object> metadata) {
        int termSize = randomIntBetween(1, 10);
        return new InternalMultiTerms(
            name,
            randomBucketOrder(),
            randomBucketOrder(),
            randomIntBetween(1, 1000),
            randomIntBetween(0, 1000),
            randomIntBetween(1, 1000),
            randomBoolean(),
            randomNonNegativeLong(),
            randomBuckets(),
            randomNonNegativeLong(),
            randomFormats(termSize),
            randomKeyConverters(termSize),
            metadata
        );
    }

    @Override
    protected List<InternalMultiTerms> randomResultsToReduce(String name, int size) {
        List<InternalMultiTerms> terms = new ArrayList<>();
        BucketOrder reduceOrder = BucketOrder.key(true);
        BucketOrder order = BucketOrder.key(true);
        int requiredSize = 10;
        long minDocCount = 1;
        int shardSize = 10;
        boolean showTermDocCountError = randomBoolean();
        int fieldCount = randomIntBetween(1, 10);
        List<DocValueFormat> formats = randomFormats(fieldCount);
        List<InternalMultiTerms.KeyConverter> keyConverters = Collections.nCopies(fieldCount, LONG);
        List<List<Object>> bucketKeys = new ArrayList<>();
        for (int i = 0; i < shardSize; i++) {
            List<Object> key;
            do {
                key = randomList(fieldCount, fieldCount, ESTestCase::randomLong);
            } while (bucketKeys.contains(key));
            bucketKeys.add(key);
        }

        for (int i = 0; i < size; i++) {
            long otherDocCount = randomLongBetween(0, Long.MAX_VALUE / size);
            List<InternalMultiTerms.Bucket> bucketList = new ArrayList<>();
            int numberOfBuckets = randomIntBetween(0, shardSize);
            List<List<Object>> visitedKeys = new ArrayList<>(randomSubsetOf(numberOfBuckets, bucketKeys));
            visitedKeys.sort(InternalMultiTerms.TERMS_COMPARATOR);
            for (int j = 0; j < numberOfBuckets; j++) {
                long docCount = randomLongBetween(0, Long.MAX_VALUE / (size * numberOfBuckets));
                long docCountError = showTermDocCountError ? randomLongBetween(0, Long.MAX_VALUE / (size * numberOfBuckets)) : -1;
                bucketList.add(
                    new InternalMultiTerms.Bucket(
                        visitedKeys.get(j),
                        docCount,
                        InternalAggregations.EMPTY,
                        showTermDocCountError,
                        docCountError,
                        formats,
                        keyConverters
                    )
                );
            }
            long docErrorCount = -1;
            terms.add(
                new InternalMultiTerms(
                    name,
                    reduceOrder,
                    order,
                    requiredSize,
                    minDocCount,
                    shardSize,
                    showTermDocCountError,
                    otherDocCount,
                    bucketList,
                    docErrorCount,
                    formats,
                    keyConverters,
                    null
                )
            );
        }
        return terms;
    }

    @Override
    protected void assertReduced(InternalMultiTerms reduced, List<InternalMultiTerms> inputs) {
        long otherDocExpected = inputs.stream().mapToLong(a -> a.otherDocCount).sum();
        assertEquals(otherDocExpected, reduced.otherDocCount);

        Map<List<Object>, Long> bucketCounts = new HashMap<>();
        for (InternalMultiTerms input : inputs) {
            for (InternalMultiTerms.Bucket bucket : input.buckets) {
                List<Object> key = bucket.getKey();
                bucketCounts.put(key, bucketCounts.getOrDefault(key, 0L) + bucket.docCount);
            }
        }
        for (InternalMultiTerms.Bucket bucket : reduced.buckets) {
            List<Object> key = bucket.getKey();
            assertThat(bucketCounts.keySet(), hasItem(equalTo(key)));
            assertThat(bucketCounts.get(key), equalTo(bucket.docCount));
        }
    }

    @Override
    protected void assertFromXContent(InternalMultiTerms min, ParsedAggregation parsedAggregation) {
        // There is no ParsedMultiTerms yet so we cannot test it here
    }

    @Override
    protected InternalMultiTerms mutateInstance(InternalMultiTerms instance) {
        String name = instance.getName();
        Map<String, Object> metadata = instance.getMetadata();
        BucketOrder order = instance.order;
        switch (between(0, 2)) {
            case 0:
                name += randomAlphaOfLength(5);
                break;
            case 1:
                order = randomValueOtherThan(order, InternalMultiTermsTests::randomBucketOrder);
                break;
            case 2:
                if (metadata == null) {
                    metadata = new HashMap<>(1);
                } else {
                    metadata = new HashMap<>(instance.getMetadata());
                }
                metadata.put(randomAlphaOfLength(15), randomInt());
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
        }
        return new InternalMultiTerms(
            name,
            order,
            instance.reduceOrder,
            instance.requiredSize,
            instance.minDocCount,
            instance.shardSize,
            instance.showTermDocCountError,
            instance.otherDocCount,
            instance.buckets,
            instance.docCountError,
            instance.formats,
            instance.keyConverters,
            metadata
        );
    }

    @Override
    protected List<NamedXContentRegistry.Entry> getNamedXContents() {
        return CollectionUtils.appendToCopy(
            super.getNamedXContents(),
            new NamedXContentRegistry.Entry(Aggregation.class, new ParseField(MultiTermsAggregationBuilder.NAME), (p, c) -> {
                assumeTrue("There is no ParsedMultiTerms yet", false);
                return null;
            })
        );
    }

    public void testKeyConverters() {
        assertThat(
            UNSIGNED_LONG.toDouble(UNSIGNED_LONG_SHIFTED, UNSIGNED_LONG_SHIFTED.parseLong("123", false, () -> 0L)),
            closeTo(123.0, 0.0001)
        );
        assertThat(
            UNSIGNED_LONG.toDouble(UNSIGNED_LONG_SHIFTED, UNSIGNED_LONG_SHIFTED.parseLong("9223372036854775813", false, () -> 0L)),
            closeTo(9223372036854775813.0, 0.0001)
        );
    }

    public void testReduceWithDoublePromotion() {
        MockBigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
        ScriptService mockScriptService = mockScriptService();
        List<DocValueFormat> formats1 = org.elasticsearch.core.List.of(DocValueFormat.RAW, UNSIGNED_LONG_SHIFTED, DocValueFormat.RAW);
        List<DocValueFormat> formats2 = org.elasticsearch.core.List.of(DocValueFormat.RAW, DocValueFormat.RAW, DocValueFormat.RAW);
        List<InternalMultiTerms.KeyConverter> keyConverters1 = org.elasticsearch.core.List.of(LONG, UNSIGNED_LONG, LONG);
        List<InternalMultiTerms.KeyConverter> keyConverters2 = org.elasticsearch.core.List.of(LONG, LONG, DOUBLE);
        BucketOrder order = BucketOrder.compound(BucketOrder.count(false), BucketOrder.key(true));
        InternalAggregations subs = InternalAggregations.EMPTY;

        InternalMultiTerms terms1 = new InternalMultiTerms(
            "test",
            order,
            order,
            10,
            1,
            10,
            false,
            0,
            org.elasticsearch.core.List.of(
                new InternalMultiTerms.Bucket(
                    org.elasticsearch.core.List.of(3L, ul("9223372036854775813"), 3L),
                    1,
                    subs,
                    false,
                    0,
                    formats1,
                    keyConverters1
                ),
                new InternalMultiTerms.Bucket(
                    org.elasticsearch.core.List.of(4L, ul("9223372036854775805"), 1L),
                    1,
                    subs,
                    false,
                    0,
                    formats1,
                    keyConverters1
                ),
                new InternalMultiTerms.Bucket(
                    org.elasticsearch.core.List.of(4L, ul("9223372036854775805"), 1L),
                    1,
                    subs,
                    false,
                    0,
                    formats1,
                    keyConverters1
                ),
                new InternalMultiTerms.Bucket(
                    org.elasticsearch.core.List.of(4L, ul("9223372036854775814"), 1L),
                    1,
                    subs,
                    false,
                    0,
                    formats1,
                    keyConverters1
                )
            ),
            0,
            formats1,
            keyConverters1,
            null
        );

        InternalMultiTerms terms2 = new InternalMultiTerms(
            "test",
            order,
            order,
            10,
            1,
            10,
            false,
            0,
            org.elasticsearch.core.List.of(
                new InternalMultiTerms.Bucket(
                    org.elasticsearch.core.List.of(3L, 9223372036854775803L, 3.0),
                    1,
                    subs,
                    false,
                    0,
                    formats2,
                    keyConverters2
                ),
                new InternalMultiTerms.Bucket(
                    org.elasticsearch.core.List.of(4L, 9223372036854775804L, 4.0),
                    1,
                    subs,
                    false,
                    0,
                    formats2,
                    keyConverters2
                ),
                new InternalMultiTerms.Bucket(
                    org.elasticsearch.core.List.of(4L, 9223372036854775805L, 4.0),
                    1,
                    subs,
                    false,
                    0,
                    formats2,
                    keyConverters2
                ),
                new InternalMultiTerms.Bucket(
                    org.elasticsearch.core.List.of(4L, 9223372036854775805L, 4.0),
                    1,
                    subs,
                    false,
                    0,
                    formats2,
                    keyConverters2
                )

            ),
            0,
            formats2,
            keyConverters2,
            null
        );
        InternalAggregation.ReduceContext context = InternalAggregation.ReduceContext.forPartialReduction(
            bigArrays,
            mockScriptService,
            () -> PipelineAggregator.PipelineTree.EMPTY
        );

        InternalMultiTerms result = (InternalMultiTerms) terms1.reduce(org.elasticsearch.core.List.of(terms1, terms2), context);
        assertThat(result.buckets, hasSize(3));
        assertThat(result.buckets.get(0).getKeyAsString(), equalTo("4|9.223372036854776E18|4.0"));
        assertThat(result.buckets.get(0).getDocCount(), equalTo(3L));
        assertThat(result.buckets.get(1).getKeyAsString(), equalTo("4|9.223372036854776E18|1.0"));
        assertThat(result.buckets.get(1).getDocCount(), equalTo(3L));
        assertThat(result.buckets.get(2).getKeyAsString(), equalTo("3|9.223372036854776E18|3.0"));
        assertThat(result.buckets.get(2).getDocCount(), equalTo(2L));
    }

    long ul(String val) {
        return UNSIGNED_LONG_SHIFTED.parseLong(val, false, () -> 0L);
    }
}
