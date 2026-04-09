/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.core.Releasables;

import static org.hamcrest.Matchers.equalTo;

public class HllStatesTests extends ComputeTestCase {

    public void testGroupingStateSerializedMergeRoundTrip() {
        BlockFactory blockFactory = blockFactory();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);

        final int precision = 1 << between(4, 14);
        final boolean sparse = randomBoolean();
        try (
            var source = new HllStates.GroupingState(driverContext, precision);
            var target = new HllStates.GroupingState(driverContext, precision)
        ) {
            final int numGroups = sparse ? between(1000, 100_000) : between(1, 100);
            for (int g = 0; g < numGroups; g++) {
                int numValues = sparse ? between(1, 64) : between(1, 10_000);
                for (int v = 0; v < numValues; v++) {
                    source.collect(g, sparse ? randomIntBetween(1, 1000) : randomIntBetween(1, 100_000));
                }
            }
            Block[] intermediates = new Block[1];
            try {
                try (IntVector selected = blockFactory.newIntRangeVector(0, numGroups)) {
                    source.toIntermediate(intermediates, 0, selected, driverContext);
                }
                BytesRef scratch = new BytesRef();
                BytesRefBlock serializedBlock = (BytesRefBlock) intermediates[0];
                for (int g = 0; g < numGroups; g++) {
                    target.merge(g, serializedBlock.getBytesRef(g, scratch), 0);
                }
            } finally {
                Releasables.close(intermediates);
            }

            for (int g = 0; g < numGroups; g++) {
                assertThat(target.cardinality(g), equalTo(source.cardinality(g)));
            }
        }
    }

}
