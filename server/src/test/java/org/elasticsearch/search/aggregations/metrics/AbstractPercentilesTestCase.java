/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.Strings;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregation.CommonFields;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public abstract class AbstractPercentilesTestCase<T extends InternalAggregation & Iterable<Percentile>> extends InternalAggregationTestCase<
    T> {
    @Override
    protected T createTestInstance(String name, Map<String, Object> metadata) {
        return createTestInstance(name, metadata, randomBoolean(), randomNumericDocValueFormat(), randomPercents(false));
    }

    @Override
    protected BuilderAndToReduce<T> randomResultsToReduce(String name, int size) {
        boolean keyed = randomBoolean();
        DocValueFormat format = randomNumericDocValueFormat();
        double[] percents = randomPercents(false);
        return new BuilderAndToReduce<T>(
            mock(AggregationBuilder.class),
            Stream.generate(() -> createTestInstance(name, null, keyed, format, percents)).limit(size).collect(toList())
        );
    }

    private T createTestInstance(String name, Map<String, Object> metadata, boolean keyed, DocValueFormat format, double[] percents) {
        int numValues = frequently() ? randomInt(100) : 0;
        double[] values = new double[numValues];
        for (int i = 0; i < numValues; ++i) {
            values[i] = randomDouble();
        }
        return createTestInstance(name, metadata, keyed, format, percents, values, false);
    }

    protected abstract T createTestInstance(
        String name,
        Map<String, Object> metadata,
        boolean keyed,
        DocValueFormat format,
        double[] percents,
        double[] values,
        boolean empty
    );

    protected abstract Class<? extends ParsedPercentiles> implementationClass();

    public void testPercentilesIterators() throws IOException {
        final T aggregation = createTestInstance();
        final Iterable<Percentile> parsedAggregation = parseAndAssert(aggregation, false, false);

        Iterator<Percentile> it = aggregation.iterator();
        Iterator<Percentile> parsedIt = parsedAggregation.iterator();
        while (it.hasNext()) {
            assertEquals(it.next(), parsedIt.next());
        }
    }

    public static double[] randomPercents(boolean sorted) {
        List<Double> randomCdfValues = randomSubsetOf(randomIntBetween(1, 7), 0.01d, 0.05d, 0.25d, 0.50d, 0.75d, 0.95d, 0.99d);
        double[] percents = new double[randomCdfValues.size()];
        for (int i = 0; i < randomCdfValues.size(); i++) {
            percents[i] = randomCdfValues.get(i);
        }
        if (sorted) {
            Arrays.sort(percents);
        }
        return percents;
    }

    @Override
    protected Predicate<String> excludePathsFromXContentInsertion() {
        return path -> path.endsWith(CommonFields.VALUES.getPreferredName());
    }

    protected abstract void assertPercentile(T agg, Double value);

    public void testEmptyRanksXContent() throws IOException {
        double[] percents = new double[] { 1, 2, 3 };
        boolean keyed = randomBoolean();
        DocValueFormat docValueFormat = randomNumericDocValueFormat();

        T agg = createTestInstance("test", Collections.emptyMap(), keyed, docValueFormat, percents, new double[0], false);

        for (Percentile percentile : agg) {
            Double value = percentile.getValue();
            assertPercentile(agg, value);
        }

        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        builder.startObject();
        agg.doXContentBody(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        String expected;
        if (keyed) {
            expected = """
                {
                  "values" : {
                    "1.0" : null,
                    "2.0" : null,
                    "3.0" : null
                  }
                }""";
        } else {
            expected = """
                {
                  "values" : [
                    {
                      "key" : 1.0,
                      "value" : null
                    },
                    {
                      "key" : 2.0,
                      "value" : null
                    },
                    {
                      "key" : 3.0,
                      "value" : null
                    }
                  ]
                }""";
        }

        assertThat(Strings.toString(builder), equalTo(expected));
    }

    public void testEmptyIterator() {
        final double[] percents = randomPercents(false);

        final Iterable<?> aggregation = createTestInstance(
            "test",
            emptyMap(),
            false,
            randomNumericDocValueFormat(),
            percents,
            new double[] {},
            true
        );

        for (var ignored : aggregation) {
            fail("empty expected");
        }

        final Iterator<?> it = aggregation.iterator();
        assertFalse(it.hasNext());
        expectThrows(NoSuchElementException.class, it::next);
    }
}
