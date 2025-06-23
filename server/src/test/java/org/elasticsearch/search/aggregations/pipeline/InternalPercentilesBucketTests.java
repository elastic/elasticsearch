/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.metrics.Percentile;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.search.aggregations.metrics.InternalPercentilesTestCase.randomPercents;
import static org.hamcrest.Matchers.equalTo;

public class InternalPercentilesBucketTests extends InternalAggregationTestCase<InternalPercentilesBucket> {

    @Override
    protected InternalPercentilesBucket createTestInstance(String name, Map<String, Object> metadata) {
        return createTestInstance(name, metadata, randomPercents(), true);
    }

    private static InternalPercentilesBucket createTestInstance(
        String name,
        Map<String, Object> metadata,
        double[] percents,
        boolean keyed
    ) {
        final double[] percentiles = new double[percents.length];
        for (int i = 0; i < percents.length; ++i) {
            percentiles[i] = frequently() ? randomDouble() : Double.NaN;
        }
        return createTestInstance(name, metadata, percents, percentiles, keyed);
    }

    private static InternalPercentilesBucket createTestInstance(
        String name,
        Map<String, Object> metadata,
        double[] percents,
        double[] percentiles,
        boolean keyed
    ) {
        DocValueFormat format = randomNumericDocValueFormat();
        return new InternalPercentilesBucket(name, percents, percentiles, keyed, format, metadata);
    }

    @Override
    public void testReduceRandom() {
        expectThrows(UnsupportedOperationException.class, () -> createTestInstance("name", null).getReducer(null, 0));
    }

    @Override
    protected void assertReduced(InternalPercentilesBucket reduced, List<InternalPercentilesBucket> inputs) {
        // no test since reduce operation is unsupported
    }

    /**
     * check that we don't rely on the percent array order and that the iterator returns the values in the original order
     */
    public void testPercentOrder() {
        final double[] percents = new double[] { 0.50, 0.25, 0.01, 0.99, 0.60 };
        InternalPercentilesBucket aggregation = createTestInstance("test", Collections.emptyMap(), percents, randomBoolean());
        Iterator<Percentile> iterator = aggregation.iterator();
        Iterator<String> nameIterator = aggregation.valueNames().iterator();
        for (double percent : percents) {
            assertTrue(iterator.hasNext());
            assertTrue(nameIterator.hasNext());

            Percentile percentile = iterator.next();
            String percentileName = nameIterator.next();

            assertEquals(percent, percentile.percent(), 0.0d);
            assertEquals(percent, Double.valueOf(percentileName), 0.0d);

            assertEquals(aggregation.percentile(percent), percentile.value(), 0.0d);
        }
        assertFalse(iterator.hasNext());
        assertFalse(nameIterator.hasNext());
    }

    public void testErrorOnDifferentArgumentSize() {
        final double[] percents = new double[] { 0.1, 0.2, 0.3 };
        final double[] percentiles = new double[] { 0.10, 0.2 };
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new InternalPercentilesBucket("test", percents, percentiles, randomBoolean(), DocValueFormat.RAW, Collections.emptyMap())
        );
        assertEquals(
            "The number of provided percents and percentiles didn't match. percents: [0.1, 0.2, 0.3], percentiles: [0.1, 0.2]",
            e.getMessage()
        );
    }

    public void testEmptyRanksXContent() throws IOException {
        double[] percents = new double[] { 1, 2, 3 };
        double[] percentiles = new double[3];
        for (int i = 0; i < 3; ++i) {
            percentiles[i] = randomBoolean() ? Double.NaN : Double.POSITIVE_INFINITY;
        }
        boolean keyed = randomBoolean();

        InternalPercentilesBucket agg = createTestInstance("test", Collections.emptyMap(), percents, percentiles, keyed);

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

    @Override
    protected InternalPercentilesBucket mutateInstance(InternalPercentilesBucket instance) {
        String name = instance.getName();
        double[] percents = extractPercents(instance);
        double[] percentiles = extractPercentiles(instance);
        DocValueFormat formatter = instance.formatter();
        Map<String, Object> metadata = instance.getMetadata();
        switch (between(0, 3)) {
            case 0 -> name += randomAlphaOfLength(5);
            case 1 -> {
                percents = Arrays.copyOf(percents, percents.length);
                percents[percents.length - 1] = randomDouble();
            }
            case 2 -> {
                percentiles = Arrays.copyOf(percentiles, percentiles.length);
                percentiles[percentiles.length - 1] = randomDouble();
            }
            case 3 -> {
                if (metadata == null) {
                    metadata = Maps.newMapWithExpectedSize(1);
                } else {
                    metadata = new HashMap<>(instance.getMetadata());
                }
                metadata.put(randomAlphaOfLength(15), randomInt());
            }
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new InternalPercentilesBucket(name, percents, percentiles, randomBoolean(), formatter, metadata);
    }

    private double[] extractPercentiles(InternalPercentilesBucket instance) {
        List<Double> values = new ArrayList<>();
        instance.iterator().forEachRemaining(percentile -> values.add(percentile.value()));
        double[] valuesArray = new double[values.size()];
        for (int i = 0; i < values.size(); i++) {
            valuesArray[i] = values.get(i);
        }
        return valuesArray;
    }

    private double[] extractPercents(InternalPercentilesBucket instance) {
        List<Double> percents = new ArrayList<>();
        instance.iterator().forEachRemaining(percentile -> percents.add(percentile.percent()));
        double[] percentArray = new double[percents.size()];
        for (int i = 0; i < percents.size(); i++) {
            percentArray[i] = percents.get(i);
        }
        return percentArray;
    }
}
