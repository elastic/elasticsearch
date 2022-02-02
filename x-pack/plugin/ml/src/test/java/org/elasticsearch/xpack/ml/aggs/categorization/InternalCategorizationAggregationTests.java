/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.categorization;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation;
import org.elasticsearch.test.InternalMultiBucketAggregationTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InternalCategorizationAggregationTests extends InternalMultiBucketAggregationTestCase<InternalCategorizationAggregation> {

    @Override
    protected SearchPlugin registerPlugin() {
        return new MachineLearning(Settings.EMPTY);
    }

    @Override
    protected List<NamedXContentRegistry.Entry> getNamedXContents() {
        return CollectionUtils.appendToCopy(
            super.getNamedXContents(),
            new NamedXContentRegistry.Entry(
                Aggregation.class,
                new ParseField(CategorizeTextAggregationBuilder.NAME),
                (p, c) -> ParsedCategorization.fromXContent(p, (String) c)
            )
        );
    }

    @Override
    protected void assertReduced(InternalCategorizationAggregation reduced, List<InternalCategorizationAggregation> inputs) {
        Map<Object, Long> reducedCounts = toCounts(reduced.getBuckets().stream());
        Map<Object, Long> totalCounts = toCounts(inputs.stream().map(InternalCategorizationAggregation::getBuckets).flatMap(List::stream));

        Map<Object, Long> expectedReducedCounts = new HashMap<>(totalCounts);
        expectedReducedCounts.keySet().retainAll(reducedCounts.keySet());
        assertEquals(expectedReducedCounts, reducedCounts);
    }

    @Override
    protected Predicate<String> excludePathsFromXContentInsertion() {
        return p -> p.contains("key");
    }

    static InternalCategorizationAggregation.BucketKey randomKey() {
        int numVals = randomIntBetween(1, 50);
        return new InternalCategorizationAggregation.BucketKey(
            Stream.generate(() -> randomAlphaOfLength(10)).limit(numVals).map(BytesRef::new).toArray(BytesRef[]::new)
        );
    }

    @Override
    protected InternalCategorizationAggregation createTestInstance(
        String name,
        Map<String, Object> metadata,
        InternalAggregations aggregations
    ) {
        List<InternalCategorizationAggregation.Bucket> buckets = new ArrayList<>();
        final int numBuckets = randomNumberOfBuckets();
        HashSet<InternalCategorizationAggregation.BucketKey> keys = new HashSet<>();
        for (int i = 0; i < numBuckets; ++i) {
            InternalCategorizationAggregation.BucketKey key = randomValueOtherThanMany(
                l -> keys.add(l) == false,
                InternalCategorizationAggregationTests::randomKey
            );
            int docCount = randomIntBetween(1, 100);
            buckets.add(new InternalCategorizationAggregation.Bucket(key, docCount, aggregations));
        }
        Collections.sort(buckets);
        return new InternalCategorizationAggregation(
            name,
            randomIntBetween(10, 100),
            randomLongBetween(1, 10),
            randomIntBetween(1, 500),
            randomIntBetween(1, 10),
            randomIntBetween(1, 100),
            metadata,
            buckets
        );
    }

    @Override
    protected Class<? extends ParsedMultiBucketAggregation<?>> implementationClass() {
        return ParsedCategorization.class;
    }

    private static Map<Object, Long> toCounts(Stream<? extends InternalCategorizationAggregation.Bucket> buckets) {
        return buckets.collect(
            Collectors.toMap(
                InternalCategorizationAggregation.Bucket::getKey,
                InternalCategorizationAggregation.Bucket::getDocCount,
                Long::sum
            )
        );
    }
}
