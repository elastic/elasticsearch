/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.search.DocValueFormat;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InternalTDigestPercentilesRanksTests extends InternalPercentilesRanksTestCase<InternalTDigestPercentileRanks> {

    @Override
    protected InternalTDigestPercentileRanks createTestInstance(String name, Map<String, Object> metadata,
                                                                boolean keyed, DocValueFormat format, double[] percents, double[] values) {
        final TDigestState state = new TDigestState(100);
        Arrays.stream(values).forEach(state::add);

        assertEquals(state.centroidCount(), values.length);
        return new InternalTDigestPercentileRanks(name, percents, state, keyed, format, metadata);
    }

    @Override
    protected void assertReduced(InternalTDigestPercentileRanks reduced, List<InternalTDigestPercentileRanks> inputs) {
        // it is hard to check the values due to the inaccuracy of the algorithm
        // the min/max values should be accurate due to the way the algo works so we can at least test those
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        long totalCount = 0;
        for (InternalTDigestPercentileRanks ranks : inputs) {
            if (ranks.state.centroidCount() == 0) {
                // quantiles would return NaN
                continue;
            }
            totalCount += ranks.state.size();
            min = Math.min(ranks.state.quantile(0), min);
            max = Math.max(ranks.state.quantile(1), max);
        }
        assertEquals(totalCount, reduced.state.size());
        if (totalCount > 0) {
            assertEquals(reduced.state.quantile(0), min, 0d);
            assertEquals(reduced.state.quantile(1), max, 0d);
        }
    }

    @Override
    protected Class<? extends ParsedPercentiles> implementationClass() {
        return ParsedTDigestPercentileRanks.class;
    }

    @Override
    protected InternalTDigestPercentileRanks mutateInstance(InternalTDigestPercentileRanks instance) {
        String name = instance.getName();
        double[] percents = instance.keys;
        TDigestState state = instance.state;
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
            TDigestState newState = new TDigestState(state.compression());
            newState.add(state);
            for (int i = 0; i < between(10, 100); i++) {
                newState.add(randomDouble());
            }
            state = newState;
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
        return new InternalTDigestPercentileRanks(name, percents, state, keyed, formatter, metadata);
    }
}
