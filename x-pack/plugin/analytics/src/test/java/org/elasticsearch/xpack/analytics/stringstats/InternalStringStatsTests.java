/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.stringstats;

import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class InternalStringStatsTests extends InternalAggregationTestCase<InternalStringStats> {

    @Override
    protected SearchPlugin registerPlugin() {
        return new AnalyticsPlugin();
    }

    @Override
    protected InternalStringStats createTestInstance(String name, Map<String, Object> metadata) {
        return createTestInstance(name, metadata, Long.MAX_VALUE, Long.MAX_VALUE);
    }

    @Override
    protected BuilderAndToReduce<InternalStringStats> randomResultsToReduce(String name, int size) {
        /*
         * Pick random count and length that are less than
         * Long.MAX_VALUE because reduction adds them together and sometimes
         * serializes them and that serialization would fail if the sum has
         * wrapped to a negative number.
         */
        return new BuilderAndToReduce<>(
            mock(AggregationBuilder.class),
            Stream.generate(() -> createTestInstance(name, null, Long.MAX_VALUE / size, Long.MAX_VALUE / size))
                .limit(size)
                .collect(toList())
        );
    }

    private InternalStringStats createTestInstance(String name, Map<String, Object> metadata, long maxCount, long maxTotalLength) {
        if (randomBoolean()) {
            return new InternalStringStats(name, 0, 0, 0, 0, emptyMap(), randomBoolean(), DocValueFormat.RAW, metadata);
        }
        long count = randomLongBetween(1, maxCount);
        long totalLength = randomLongBetween(0, maxTotalLength);
        return new InternalStringStats(
            name,
            count,
            totalLength,
            between(0, Integer.MAX_VALUE),
            between(0, Integer.MAX_VALUE),
            randomCharOccurrences(),
            randomBoolean(),
            DocValueFormat.RAW,
            metadata
        );
    }

    @Override
    protected InternalStringStats mutateInstance(InternalStringStats instance) {
        String name = instance.getName();
        long count = instance.getCount();
        long totalLength = instance.getTotalLength();
        int minLength = instance.getMinLength();
        int maxLength = instance.getMaxLength();
        Map<String, Long> charOccurrences = instance.getCharOccurrences();
        boolean showDistribution = instance.getShowDistribution();
        switch (between(0, 6)) {
            case 0 -> name = name + "a";
            case 1 -> count = randomValueOtherThan(count, () -> randomLongBetween(1, Long.MAX_VALUE));
            case 2 -> totalLength = randomValueOtherThan(totalLength, ESTestCase::randomNonNegativeLong);
            case 3 -> minLength = randomValueOtherThan(minLength, () -> between(0, Integer.MAX_VALUE));
            case 4 -> maxLength = randomValueOtherThan(maxLength, () -> between(0, Integer.MAX_VALUE));
            case 5 -> charOccurrences = randomValueOtherThan(charOccurrences, this::randomCharOccurrences);
            case 6 -> showDistribution = showDistribution == false;
        }
        return new InternalStringStats(
            name,
            count,
            totalLength,
            minLength,
            maxLength,
            charOccurrences,
            showDistribution,
            DocValueFormat.RAW,
            instance.getMetadata()
        );
    }

    @Override
    protected void assertReduced(InternalStringStats reduced, List<InternalStringStats> inputs) {
        assertThat(reduced.getCount(), equalTo(inputs.stream().mapToLong(InternalStringStats::getCount).sum()));
        assertThat(reduced.getMinLength(), equalTo(inputs.stream().mapToInt(InternalStringStats::getMinLength).min().getAsInt()));
        assertThat(reduced.getMaxLength(), equalTo(inputs.stream().mapToInt(InternalStringStats::getMaxLength).max().getAsInt()));
        assertThat(reduced.getTotalLength(), equalTo(inputs.stream().mapToLong(InternalStringStats::getTotalLength).sum()));
        Map<String, Long> reducedChars = new HashMap<>();
        for (InternalStringStats stats : inputs) {
            for (Map.Entry<String, Long> e : stats.getCharOccurrences().entrySet()) {
                reducedChars.merge(e.getKey(), e.getValue(), (lhs, rhs) -> lhs + rhs);
            }
        }
        assertThat(reduced.getCharOccurrences(), equalTo(reducedChars));
    }

    private Map<String, Long> randomCharOccurrences() {
        Map<String, Long> charOccurrences = new HashMap<String, Long>();
        int occurrencesSize = between(0, 1000);
        while (charOccurrences.size() < occurrencesSize) {
            charOccurrences.put(randomAlphaOfLength(5), randomNonNegativeLong());
        }
        return charOccurrences;
    }
}
