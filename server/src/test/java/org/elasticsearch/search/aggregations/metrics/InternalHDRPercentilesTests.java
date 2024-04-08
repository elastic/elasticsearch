/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.HdrHistogram.DoubleHistogram;
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

public class InternalHDRPercentilesTests extends InternalPercentilesTestCase<InternalHDRPercentiles> {

    @Override
    protected InternalHDRPercentiles createTestInstance(
        String name,
        Map<String, Object> metadata,
        boolean keyed,
        DocValueFormat format,
        double[] percents,
        double[] values,
        boolean empty
    ) {

        if (empty) {
            return new InternalHDRPercentiles(name, percents, null, keyed, format, metadata);
        }

        final DoubleHistogram state = new DoubleHistogram(3);
        Arrays.stream(values).forEach(state::recordValue);

        return new InternalHDRPercentiles(name, percents, state, keyed, format, metadata);
    }

    @Override
    protected void assertReduced(InternalHDRPercentiles reduced, List<InternalHDRPercentiles> inputs) {
        // it is hard to check the values due to the inaccuracy of the algorithm
        long totalCount = 0;
        for (InternalHDRPercentiles ranks : inputs) {
            totalCount += ranks.state.getTotalCount();
        }
        assertEquals(totalCount, reduced.state.getTotalCount());
    }

    @Override
    protected boolean supportsSampling() {
        return true;
    }

    @Override
    protected void assertSampled(InternalHDRPercentiles sampled, InternalHDRPercentiles reduced, SamplingContext samplingContext) {
        Iterator<Percentile> it1 = sampled.iterator();
        Iterator<Percentile> it2 = reduced.iterator();
        while (it1.hasNext() && it2.hasNext()) {
            assertThat(it1.next(), equalTo(it2.next()));
        }
    }

    public void testIterator() {
        final double[] percents = randomPercents(false);
        final double[] values = new double[frequently() ? randomIntBetween(1, 10) : 0];
        for (int i = 0; i < values.length; ++i) {
            values[i] = randomDouble();
        }

        InternalHDRPercentiles aggregation = createTestInstance(
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
            assertTrue(nameIterator.hasNext());

            Percentile percentile = iterator.next();
            String percentileName = nameIterator.next();

            assertEquals(percent, Double.valueOf(percentileName), 0.0d);
            assertEquals(percent, percentile.percent(), 0.0d);

            assertEquals(aggregation.percentile(percent), percentile.value(), 0.0d);
            assertEquals(aggregation.value(String.valueOf(percent)), percentile.value(), 0.0d);
        }
        assertFalse(iterator.hasNext());
        assertFalse(nameIterator.hasNext());
    }

    @Override
    protected InternalHDRPercentiles mutateInstance(InternalHDRPercentiles instance) {
        String name = instance.getName();
        double[] percents = instance.keys;
        DoubleHistogram state = instance.state;
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
                state = new DoubleHistogram(state);
                for (int i = 0; i < between(10, 100); i++) {
                    state.recordValue(randomDouble());
                }
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
        return new InternalHDRPercentiles(name, percents, state, keyed, formatter, metadata);
    }
}
