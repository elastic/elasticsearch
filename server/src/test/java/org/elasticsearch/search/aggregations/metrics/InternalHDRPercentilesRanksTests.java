/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.HdrHistogram.DoubleHistogram;
import org.elasticsearch.search.DocValueFormat;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class InternalHDRPercentilesRanksTests extends InternalPercentilesRanksTestCase<InternalHDRPercentileRanks> {

    @Override
    protected InternalHDRPercentileRanks createTestInstance(String name, Map<String, Object> metadata,
                                                            boolean keyed, DocValueFormat format, double[] percents, double[] values) {

        final DoubleHistogram state = new DoubleHistogram(3);
        Arrays.stream(values).forEach(state::recordValue);

        return new InternalHDRPercentileRanks(name, percents, state, keyed, format, metadata);
    }

    @Override
    protected void assertReduced(InternalHDRPercentileRanks reduced, List<InternalHDRPercentileRanks> inputs) {
        // it is hard to check the values due to the inaccuracy of the algorithm
        long totalCount = 0;
        for (InternalHDRPercentileRanks ranks : inputs) {
            totalCount += ranks.state.getTotalCount();
        }
        assertEquals(totalCount, reduced.state.getTotalCount());
    }

    @Override
    protected Class<? extends ParsedPercentiles> implementationClass() {
        return ParsedHDRPercentileRanks.class;
    }

    @Override
    protected InternalHDRPercentileRanks mutateInstance(InternalHDRPercentileRanks instance) {
        String name = instance.getName();
        double[] percents = instance.keys;
        DoubleHistogram state = instance.state;
        boolean keyed = instance.keyed;
        DocValueFormat formatter = instance.formatter();
        Map<String, Object> metadata = instance.getMetadata();
        switch (between(0, 4)) {
        case 0:
            name += randomAlphaOfLength(5);
            break;
        case 1:
            percents = Arrays.copyOf(percents, percents.length + 1);
            percents[percents.length - 1] = randomDouble() * 100;
            Arrays.sort(percents);
            break;
        case 2:
            state = new DoubleHistogram(state);
            for (int i = 0; i < between(10, 100); i++) {
                state.recordValue(randomDouble());
            }
            break;
        case 3:
            keyed = keyed == false;
            break;
        case 4:
            if (metadata == null) {
                metadata = new HashMap<>(1);
            } else {
                metadata = new HashMap<>(instance.getMetadata());
            }
            metadata.put(randomAlphaOfLength(15), randomInt());
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }
        return new InternalHDRPercentileRanks(name, percents, state, keyed, formatter, metadata);
    }
}
