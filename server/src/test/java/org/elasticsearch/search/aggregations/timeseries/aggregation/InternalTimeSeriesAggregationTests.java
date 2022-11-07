/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.timeseries.aggregation;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation;
import org.elasticsearch.search.aggregations.timeseries.aggregation.InternalTimeSeriesAggregation.InternalBucket;
import org.elasticsearch.search.aggregations.timeseries.aggregation.internal.TimeSeriesLineAggreagation;
import org.elasticsearch.test.InternalMultiBucketAggregationTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Predicate;

public class InternalTimeSeriesAggregationTests extends InternalMultiBucketAggregationTestCase<InternalTimeSeriesAggregation> {

    @Override
    protected void assertReduced(InternalTimeSeriesAggregation reduced, List<InternalTimeSeriesAggregation> inputs) {
        // TODO
    }

    @Override
    protected InternalTimeSeriesAggregation createTestInstance(
        String name,
        Map<String, Object> metadata,
        InternalAggregations aggregations
    ) {
        BucketOrder order = BucketOrder.count(false);
        long minDocCount = 1;
        int requiredSize = 3;
        int shardSize = requiredSize;
        long otherDocCount = 0;
        boolean keyed = randomBoolean();
        boolean showTermDocCountError = randomBoolean();
        List<InternalBucket> buckets = randomBuckets(keyed, shardSize, showTermDocCountError, requiredSize);
        BucketOrder reduceOrder = randomBoolean()
            ? BucketOrder.compound(BucketOrder.key(true), BucketOrder.count(false))
            : BucketOrder.key(true);
        Collections.sort(buckets, reduceOrder.comparator());
        return new InternalTimeSeriesAggregation(
            name,
            reduceOrder,
            order,
            requiredSize,
            minDocCount,
            shardSize,
            showTermDocCountError,
            otherDocCount,
            buckets,
            keyed,
            randomNonNegativeLong(),
            metadata
        );
    }

    @Override
    protected Class<? extends ParsedMultiBucketAggregation<?>> implementationClass() {
        return ParsedTimeSeriesAggregation.class;
    }

    private List<InternalTimeSeriesAggregation.InternalBucket> randomBuckets(
        boolean keyed,
        int shardSize,
        boolean showTermDocCountError,
        int size
    ) {
        int numberOfBuckets = randomIntBetween(0, shardSize);
        List<InternalTimeSeriesAggregation.InternalBucket> bucketList = new ArrayList<>(numberOfBuckets);
        List<Map<String, Object>> keys = randomKeys(bucketKeys(randomIntBetween(1, 4)), numberOfBuckets);
        for (int j = 0; j < numberOfBuckets; j++) {
            long docCount = randomLongBetween(0, Long.MAX_VALUE / (size * numberOfBuckets));
            long docCountError = showTermDocCountError ? randomLongBetween(0, Long.MAX_VALUE / (size * numberOfBuckets)) : -1;
            bucketList.add(
                new InternalTimeSeriesAggregation.InternalBucket(
                    keys.get(j),
                    docCount,
                    new TimeSeriesLineAggreagation(
                        TimeSeriesLineAggreagation.NAME,
                        Collections.emptyMap(),
                        DocValueFormat.RAW,
                        Collections.emptyMap()
                    ),
                    InternalAggregations.EMPTY,
                    keyed,
                    showTermDocCountError,
                    docCountError
                )
            );
        }
        return bucketList;
    }

    private List<String> bucketKeys(int numberOfKeys) {
        return randomUnique(() -> randomAlphaOfLength(10), numberOfKeys).stream().toList();
    }

    private List<Map<String, Object>> randomKeys(List<String> bucketKeys, int numberOfBuckets) {
        List<Map<String, Object>> keys = new ArrayList<>();
        for (int i = 0; i < numberOfBuckets; i++) {
            keys.add(randomValueOtherThanMany(keys::contains, () -> {
                Map<String, Object> key = new TreeMap<>();
                for (String name : bucketKeys) {
                    key.put(name, randomAlphaOfLength(4));
                }
                return key;
            }));
        }
        return keys;
    }

    @Override
    protected Predicate<String> excludePathsFromXContentInsertion() {
        return s -> s.endsWith(".key") || s.endsWith("values");
    }
}
