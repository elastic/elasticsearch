/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.stringstats;

import org.elasticsearch.client.analytics.ParsedStringStats;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalAggregationTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class InternalStringStatsTests extends InternalAggregationTestCase<InternalStringStats> {
    @Override
    protected List<NamedXContentRegistry.Entry> getNamedXContents() {
        List<NamedXContentRegistry.Entry> result = new ArrayList<>(super.getNamedXContents());
        result.add(new NamedXContentRegistry.Entry(Aggregation.class, new ParseField(StringStatsAggregationBuilder.NAME),
                (p, c) -> ParsedStringStats.PARSER.parse(p, (String) c)));
        return result;
    }

    protected InternalStringStats createTestInstance(
            String name, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        if (randomBoolean()) {
            return new InternalStringStats(name, 0, 0, 0, 0, emptyMap(), randomBoolean(), DocValueFormat.RAW,
                    pipelineAggregators, metaData);
        }
        return new InternalStringStats(name, randomLongBetween(1, Long.MAX_VALUE),
                randomNonNegativeLong(), between(0, Integer.MAX_VALUE), between(0, Integer.MAX_VALUE), randomCharOccurrences(),
                randomBoolean(), DocValueFormat.RAW,
                pipelineAggregators, metaData);
    };

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
             showDistribution = !showDistribution;
             break;
         }
        return new InternalStringStats(name, count, totalLength, minLength, maxLength, charOccurrences, showDistribution,
                DocValueFormat.RAW, instance.pipelineAggregators(), instance.getMetaData());
    }

    @Override
    protected Reader<InternalStringStats> instanceReader() {
        return InternalStringStats::new;
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
