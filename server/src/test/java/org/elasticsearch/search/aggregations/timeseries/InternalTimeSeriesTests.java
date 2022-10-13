/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.timeseries;

import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.test.InternalMultiBucketAggregationTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.arrayContainingInAnyOrder;

public class InternalTimeSeriesTests extends InternalMultiBucketAggregationTestCase<InternalTimeSeries> {

    private List<InternalTimeSeries.InternalBucket> randomBuckets(boolean keyed, InternalAggregations aggregations) {
        int numberOfBuckets = randomNumberOfBuckets();
        List<InternalTimeSeries.InternalBucket> bucketList = new ArrayList<>(numberOfBuckets);
        List<Map<String, Object>> keys = randomKeys(bucketKeys(randomIntBetween(1, 4)), numberOfBuckets);
        for (int j = 0; j < numberOfBuckets; j++) {
            long docCount = randomLongBetween(0, Long.MAX_VALUE / (20L * numberOfBuckets));
            bucketList.add(new InternalTimeSeries.InternalBucket(keys.get(j), docCount, aggregations, keyed));
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
    protected InternalTimeSeries createTestInstance(String name, Map<String, Object> metadata, InternalAggregations aggregations) {
        boolean keyed = randomBoolean();
        return new InternalTimeSeries(name, randomBuckets(keyed, aggregations), keyed, metadata);
    }

    @Override
    protected void assertReduced(InternalTimeSeries reduced, List<InternalTimeSeries> inputs) {
        Map<Map<String, Object>, Long> keys = new HashMap<>();
        for (InternalTimeSeries in : inputs) {
            for (InternalTimeSeries.InternalBucket bucket : in.getBuckets()) {
                keys.compute(bucket.getKey(), (k, v) -> {
                    if (v == null) {
                        return bucket.docCount;
                    } else {
                        return bucket.docCount + v;
                    }
                });
            }
        }
        assertThat(
            reduced.getBuckets().stream().map(InternalTimeSeries.InternalBucket::getKey).toArray(Object[]::new),
            arrayContainingInAnyOrder(keys.keySet().toArray(Object[]::new))
        );
    }

    @Override
    protected Class<ParsedTimeSeries> implementationClass() {
        return ParsedTimeSeries.class;
    }

    @Override
    protected Predicate<String> excludePathsFromXContentInsertion() {
        return s -> s.endsWith(".key");
    }
}
