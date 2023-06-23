/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.aggregations.bucket.timeseries;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.aggregations.bucket.AggregationMultiBucketAggregationTestCase;
import org.elasticsearch.aggregations.bucket.timeseries.InternalTimeSeries.InternalBucket;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.xcontent.ContextParser;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

public class InternalTimeSeriesTests extends AggregationMultiBucketAggregationTestCase<InternalTimeSeries> {

    @Override
    protected Map.Entry<String, ContextParser<Object, Aggregation>> getParser() {
        return Map.entry(TimeSeriesAggregationBuilder.NAME, (p, c) -> ParsedTimeSeries.fromXContent(p, (String) c));
    }

    private List<InternalBucket> randomBuckets(boolean keyed, InternalAggregations aggregations) {
        int numberOfBuckets = randomNumberOfBuckets();
        List<InternalBucket> bucketList = new ArrayList<>(numberOfBuckets);
        List<Map<String, Object>> keys = randomKeys(bucketKeys(randomIntBetween(1, 4)), numberOfBuckets);
        for (int j = 0; j < numberOfBuckets; j++) {
            long docCount = randomLongBetween(0, Long.MAX_VALUE / (20L * numberOfBuckets));
            var builder = new TimeSeriesIdFieldMapper.TimeSeriesIdBuilder(null);
            for (var entry : keys.get(j).entrySet()) {
                builder.addString(entry.getKey(), (String) entry.getValue());
            }
            try {
                var key = builder.build().toBytesRef();
                bucketList.add(new InternalBucket(key, docCount, aggregations, keyed));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        // The interal time series' reduce method expects for each shard level response that the buckets are sorted by tsid:
        bucketList.sort(Comparator.comparing(o -> o.key));
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
            for (InternalBucket bucket : in.getBuckets()) {
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
            reduced.getBuckets().stream().map(InternalBucket::getKey).toArray(Object[]::new),
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

    public void testReduceSimple() {
        // a simple test, to easily spot easy mistakes in the merge logic in InternalTimeSeries#reduce(...) method.
        InternalTimeSeries first = new InternalTimeSeries(
            "ts",
            List.of(
                new InternalBucket(new BytesRef("1"), 3, InternalAggregations.EMPTY, false),
                new InternalBucket(new BytesRef("10"), 6, InternalAggregations.EMPTY, false),
                new InternalBucket(new BytesRef("2"), 2, InternalAggregations.EMPTY, false),
                new InternalBucket(new BytesRef("9"), 5, InternalAggregations.EMPTY, false)
            ),
            false,
            Map.of()
        );
        InternalTimeSeries second = new InternalTimeSeries(
            "ts",
            List.of(
                new InternalBucket(new BytesRef("2"), 1, InternalAggregations.EMPTY, false),
                new InternalBucket(new BytesRef("3"), 3, InternalAggregations.EMPTY, false)
            ),
            false,
            Map.of()
        );
        InternalTimeSeries third = new InternalTimeSeries(
            "ts",
            List.of(
                new InternalBucket(new BytesRef("1"), 2, InternalAggregations.EMPTY, false),
                new InternalBucket(new BytesRef("3"), 4, InternalAggregations.EMPTY, false),
                new InternalBucket(new BytesRef("9"), 4, InternalAggregations.EMPTY, false)
            ),
            false,
            Map.of()
        );
        AggregationReduceContext context = new AggregationReduceContext.ForFinal(
            new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService()),
            mockScriptService(),
            () -> false,
            new TimeSeriesAggregationBuilder("ts"),
            value -> {},
            PipelineAggregator.PipelineTree.EMPTY
        );

        InternalTimeSeries result = (InternalTimeSeries) first.reduce(List.of(first, second, third), context);
        assertThat(result.getBuckets().get(0).key.utf8ToString(), equalTo("1"));
        assertThat(result.getBuckets().get(0).getDocCount(), equalTo(5L));
        assertThat(result.getBuckets().get(1).key.utf8ToString(), equalTo("10"));
        assertThat(result.getBuckets().get(1).getDocCount(), equalTo(6L));
        assertThat(result.getBuckets().get(2).key.utf8ToString(), equalTo("2"));
        assertThat(result.getBuckets().get(2).getDocCount(), equalTo(3L));
        assertThat(result.getBuckets().get(3).key.utf8ToString(), equalTo("3"));
        assertThat(result.getBuckets().get(3).getDocCount(), equalTo(7L));
        assertThat(result.getBuckets().get(4).key.utf8ToString(), equalTo("9"));
        assertThat(result.getBuckets().get(4).getDocCount(), equalTo(9L));
    }

    @Override
    protected InternalTimeSeries mutateInstance(InternalTimeSeries instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }
}
