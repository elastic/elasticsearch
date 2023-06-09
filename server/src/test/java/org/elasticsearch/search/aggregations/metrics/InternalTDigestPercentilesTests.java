/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.support.SamplingContext;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.equalTo;

public class InternalTDigestPercentilesTests extends InternalPercentilesTestCase<InternalTDigestPercentiles> {

    @Override
    protected InternalTDigestPercentiles createTestInstance(
        String name,
        Map<String, Object> metadata,
        boolean keyed,
        DocValueFormat format,
        double[] percents,
        double[] values,
        boolean empty
    ) {
        if (empty) {
            return new InternalTDigestPercentiles(name, percents, null, keyed, format, metadata);
        }
        final TDigestState state = new TDigestState(100);
        Arrays.stream(values).forEach(state::add);

        assertEquals(state.centroidCount(), values.length);
        return new InternalTDigestPercentiles(name, percents, state, keyed, format, metadata);
    }

    @Override
    protected void assertReduced(InternalTDigestPercentiles reduced, List<InternalTDigestPercentiles> inputs) {
        final TDigestState expectedState = new TDigestState(reduced.state.compression());

        long totalCount = 0;
        for (InternalTDigestPercentiles input : inputs) {
            assertArrayEquals(reduced.keys, input.keys, 0d);
            expectedState.add(input.state);
            totalCount += input.state.size();
        }

        assertEquals(totalCount, reduced.state.size());
        if (totalCount > 0) {
            assertEquals(expectedState.quantile(0), reduced.state.quantile(0), 0d);
            assertEquals(expectedState.quantile(1), reduced.state.quantile(1), 0d);
        }
    }

    @Override
    protected boolean supportsSampling() {
        return true;
    }

    @Override
    protected void assertSampled(InternalTDigestPercentiles sampled, InternalTDigestPercentiles reduced, SamplingContext samplingContext) {
        Iterator<Percentile> it1 = sampled.iterator();
        Iterator<Percentile> it2 = reduced.iterator();
        while (it1.hasNext() && it2.hasNext()) {
            assertThat(it1.next(), equalTo(it2.next()));
        }
    }

    @Override
    protected Class<? extends ParsedPercentiles> implementationClass() {
        return ParsedTDigestPercentiles.class;
    }

    @Override
    protected InternalTDigestPercentiles mutateInstance(InternalTDigestPercentiles instance) {
        String name = instance.getName();
        double[] percents = instance.keys;
        TDigestState state = instance.state;
        boolean keyed = instance.keyed;
        DocValueFormat formatter = instance.formatter();
        Map<String, Object> metadata = instance.getMetadata();
        switch (between(0, 4)) {
            case 0 -> name += randomAlphaOfLength(5);
            case 1 -> {
                percents = Arrays.copyOf(percents, percents.length + 1);
                percents[percents.length - 1] = randomDouble() * 100;
                Arrays.sort(percents);
            }
            case 2 -> {
                TDigestState newState = new TDigestState(state.compression());
                newState.add(state);
                for (int i = 0; i < between(10, 100); i++) {
                    newState.add(randomDouble());
                }
                state = newState;
            }
            case 3 -> keyed = keyed == false;
            case 4 -> {
                if (metadata == null) {
                    metadata = Maps.newMapWithExpectedSize(1);
                } else {
                    metadata = new HashMap<>(instance.getMetadata());
                }
                metadata.put(randomAlphaOfLength(15), randomInt());
            }
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new InternalTDigestPercentiles(name, percents, state, keyed, formatter, metadata);
    }

    public void testIterator() {
        final double[] percents = randomPercents(false);
        final double[] values = new double[frequently() ? randomIntBetween(1, 10) : 0];
        for (int i = 0; i < values.length; ++i) {
            values[i] = randomDouble();
        }

        InternalTDigestPercentiles aggregation = createTestInstance(
            "test",
            emptyMap(),
            false,
            randomNumericDocValueFormat(),
            percents,
            values,
            false
        );

        Iterator<Percentile> iterator = aggregation.iterator();
        Iterator<String> nameIterator = aggregation.valueNames().iterator();
        for (double percent : percents) {
            assertTrue(iterator.hasNext());

            Percentile percentile = iterator.next();
            String percentileName = nameIterator.next();

            assertEquals(percent, Double.valueOf(percentileName), 0.0d);
            assertEquals(percent, percentile.getPercent(), 0.0d);

            assertEquals(aggregation.percentile(percent), percentile.getValue(), 0.0d);
            assertEquals(aggregation.value(String.valueOf(percent)), percentile.getValue(), 0.0d);
        }
        assertFalse(iterator.hasNext());
        assertFalse(nameIterator.hasNext());
    }
}
