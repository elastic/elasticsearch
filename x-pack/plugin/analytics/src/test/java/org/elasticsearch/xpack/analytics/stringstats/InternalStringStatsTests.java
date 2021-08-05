/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.stringstats;

import org.elasticsearch.client.analytics.ParsedStringStats;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class InternalStringStatsTests extends InternalAggregationTestCase<InternalStringStats> {

    @Override
    protected SearchPlugin registerPlugin() {
        return new AnalyticsPlugin();
    }

    @Override
    protected List<NamedXContentRegistry.Entry> getNamedXContents() {
        return CollectionUtils.appendToCopy(super.getNamedXContents(), new NamedXContentRegistry.Entry(Aggregation.class,
                new ParseField(StringStatsAggregationBuilder.NAME), (p, c) -> ParsedStringStats.PARSER.parse(p, (String) c)));
    }

    @Override
    protected InternalStringStats createTestInstance(String name, Map<String, Object> metadata) {
        return createTestInstance(name, metadata, Long.MAX_VALUE, Long.MAX_VALUE);
    }

    @Override
    protected List<InternalStringStats> randomResultsToReduce(String name, int size) {
        /*
         * Pick random count and length that are less than
         * Long.MAX_VALUE because reduction adds them together and sometimes
         * serializes them and that serialization would fail if the sum has
         * wrapped to a negative number.
         */
        return Stream.generate(() -> createTestInstance(name, null, Long.MAX_VALUE / size, Long.MAX_VALUE / size))
            .limit(size)
            .collect(toList());
    }

    private InternalStringStats createTestInstance(String name, Map<String, Object> metadata, long maxCount, long maxTotalLength) {
        if (randomBoolean()) {
            return new InternalStringStats(name, 0, 0, 0, 0, emptyMap(), randomBoolean(), DocValueFormat.RAW, metadata);
        }
        long count = randomLongBetween(1, maxCount);
        long totalLength = randomLongBetween(0, maxTotalLength);
        return new InternalStringStats(name, count, totalLength,
                between(0, Integer.MAX_VALUE), between(0, Integer.MAX_VALUE), randomCharOccurrences(),
                randomBoolean(), DocValueFormat.RAW, metadata);
    }

    @Override
    protected InternalStringStats mutateInstance(InternalStringStats instance) throws IOException {
         String name = instance.getName();
         long count = instance.getCount();
         long totalLength = instance.getTotalLength();
         int minLength = instance.getMinLength();
         int maxLength = instance.getMaxLength();
         Map<String, Long> charOccurrences = instance.getCharOccurrences();
         boolean showDistribution = instance.getShowDistribution();
         switch (between(0, 6)) {
         case 0:
             name = name + "a";
             break;
         case 1:
             count = randomValueOtherThan(count, () -> randomLongBetween(1, Long.MAX_VALUE));
             break;
         case 2:
             totalLength = randomValueOtherThan(totalLength, ESTestCase::randomNonNegativeLong);
             break;
         case 3:
             minLength = randomValueOtherThan(minLength, () -> between(0, Integer.MAX_VALUE));
             break;
         case 4:
             maxLength = randomValueOtherThan(maxLength, () -> between(0, Integer.MAX_VALUE));
             break;
         case 5:
             charOccurrences = randomValueOtherThan(charOccurrences, this::randomCharOccurrences);
             break;
         case 6:
             showDistribution = showDistribution == false;
             break;
         }
        return new InternalStringStats(name, count, totalLength, minLength, maxLength, charOccurrences, showDistribution,
                DocValueFormat.RAW, instance.getMetadata());
    }

    @Override
    protected void assertFromXContent(InternalStringStats aggregation, ParsedAggregation parsedAggregation) throws IOException {
        ParsedStringStats parsed = (ParsedStringStats) parsedAggregation;
        assertThat(parsed.getName(), equalTo(aggregation.getName()));
        if (aggregation.getCount() == 0) {
            assertThat(parsed.getCount(), equalTo(0L));
            assertThat(parsed.getMinLength(), equalTo(0));
            assertThat(parsed.getMaxLength(), equalTo(0));
            assertThat(parsed.getAvgLength(), equalTo(0d));
            assertThat(parsed.getEntropy(), equalTo(0d));
            assertThat(parsed.getDistribution(), nullValue());
            return;
        }
        assertThat(parsed.getCount(), equalTo(aggregation.getCount()));
        assertThat(parsed.getMinLength(), equalTo(aggregation.getMinLength()));
        assertThat(parsed.getMaxLength(), equalTo(aggregation.getMaxLength()));
        assertThat(parsed.getAvgLength(), equalTo(aggregation.getAvgLength()));
        assertThat(parsed.getEntropy(), equalTo(aggregation.getEntropy()));
        if (aggregation.getShowDistribution()) {
            assertThat(parsed.getDistribution(), equalTo(aggregation.getDistribution()));
        } else {
            assertThat(parsed.getDistribution(), nullValue());
        }
    }

    @Override
    protected Predicate<String> excludePathsFromXContentInsertion() {
        return path -> path.endsWith(".distribution");
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
