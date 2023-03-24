/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;

public class BlockMultiValuedTests extends ESTestCase {
    @ParametersFactory
    public static List<Object[]> params() throws Exception {
        List<Object[]> params = new ArrayList<>();
        for (ElementType elementType : ElementType.values()) {
            if (elementType == ElementType.UNKNOWN || elementType == ElementType.NULL || elementType == ElementType.DOC) {
                continue;
            }
            for (boolean nullAllowed : new boolean[] { false, true }) {
                params.add(new Object[] { elementType, nullAllowed });
            }
        }
        return params;
    }

    private final ElementType elementType;
    private final boolean nullAllowed;

    public BlockMultiValuedTests(@Name("elementType") ElementType elementType, @Name("nullAllowed") boolean nullAllowed) {
        this.elementType = elementType;
        this.nullAllowed = nullAllowed;
    }

    public void testMultiValued() {
        int positionCount = randomIntBetween(1, 16 * 1024);
        var b = BasicBlockTests.randomBlock(elementType, positionCount, nullAllowed, 0, 10);

        assertThat(b.block().getPositionCount(), equalTo(positionCount));
        for (int r = 0; r < positionCount; r++) {
            if (b.values().get(r) == null) {
                assertThat(b.block().getValueCount(r), equalTo(0));
                assertThat(b.block().isNull(r), equalTo(true));
            } else {
                assertThat(b.block().getValueCount(r), equalTo(b.values().get(r).size()));
                assertThat(BasicBlockTests.valuesAtPositions(b.block(), r, r + 1).get(0), equalTo(b.values().get(r)));
            }
        }
    }

    public void testFilteredNoop() {
        assertFiltered(true, false);
    }

    public void testFilteredReordered() {
        assertFiltered(true, true);
    }

    public void testFilteredSubset() {
        assertFiltered(false, false);
    }

    public void testFilteredJumbledSubset() {
        assertFiltered(false, true);
    }

    private void assertFiltered(boolean all, boolean shuffled) {
        int positionCount = randomIntBetween(1, 16 * 1024);
        var b = BasicBlockTests.randomBlock(elementType, positionCount, nullAllowed, 0, 10);

        int[] positions = IntStream.range(0, positionCount).toArray();
        if (shuffled) {
            Randomness.shuffle(Arrays.asList(positions));
        }
        if (all == false) {
            int[] pos = positions;
            positions = IntStream.range(0, between(1, positionCount)).map(i -> pos[i]).toArray();
        }
        Block filtered = b.block().filter(positions);

        assertThat(b.block().getPositionCount(), equalTo(positionCount));
        for (int r = 0; r < positions.length; r++) {
            if (b.values().get(positions[r]) == null) {
                assertThat(filtered.getValueCount(r), equalTo(0));
                assertThat(filtered.isNull(r), equalTo(true));
            } else {
                assertThat(filtered.getValueCount(r), equalTo(b.values().get(positions[r]).size()));
                assertThat(BasicBlockTests.valuesAtPositions(filtered, r, r + 1).get(0), equalTo(b.values().get(positions[r])));
            }
        }
    }
}
