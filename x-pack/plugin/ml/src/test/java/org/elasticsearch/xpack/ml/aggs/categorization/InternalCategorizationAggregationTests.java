/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.categorization;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation;
import org.elasticsearch.test.InternalMultiBucketAggregationTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

public class InternalCategorizationAggregationTests extends InternalMultiBucketAggregationTestCase<InternalCategorizationAggregation> {

    private CategorizationBytesRefHash bytesRefHash;

    @Before
    public void createHash() {
        bytesRefHash = new CategorizationBytesRefHash(new BytesRefHash(2048, BigArrays.NON_RECYCLING_INSTANCE));
    }

    @After
    public void destroyHash() {
        bytesRefHash.close();
    }

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

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/87240")
    public void testReduceRandom() {
        // The bug is in the assertReduced() method immediately below that the base class testReduceRandom() calls.
        // To unmute after the bug is fixed, simply delete this entire method so that the base class method is used again.
    }

    @Override
    protected void assertReduced(InternalCategorizationAggregation reduced, List<InternalCategorizationAggregation> inputs) {
        Map<Object, Long> reducedCounts = toCounts(reduced.getBuckets().stream());
        Map<Object, Long> totalCounts = toCounts(inputs.stream().map(InternalCategorizationAggregation::getBuckets).flatMap(List::stream));

        Map<Object, Long> expectedReducedCounts = new HashMap<>(totalCounts);
        expectedReducedCounts.keySet().retainAll(reducedCounts.keySet());
        assertThat(reducedCounts, equalTo(expectedReducedCounts));
    }

    @Override
    protected Predicate<String> excludePathsFromXContentInsertion() {
        return p -> p.contains("key");
    }

    @Override
    protected InternalCategorizationAggregation createTestInstance(
        String name,
        Map<String, Object> metadata,
        InternalAggregations aggregations
    ) {
        List<InternalCategorizationAggregation.Bucket> buckets = new ArrayList<>();
        final int numBuckets = randomNumberOfBuckets();
        for (int i = 0; i < numBuckets; ++i) {
            buckets.add(
                new InternalCategorizationAggregation.Bucket(SerializableTokenListCategoryTests.createTestInstance(bytesRefHash), -1)
            );
        }
        Collections.sort(buckets);
        return new InternalCategorizationAggregation(
            name,
            randomIntBetween(10, 100),
            randomLongBetween(1, 10),
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
