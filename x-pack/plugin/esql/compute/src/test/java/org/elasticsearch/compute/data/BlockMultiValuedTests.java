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
    public static List<Object[]> params() {
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
        var b = BasicBlockTests.randomBlock(elementType, positionCount, nullAllowed, 0, 10, 0, 0);

        assertThat(b.block().getPositionCount(), equalTo(positionCount));
        assertThat(b.block().getTotalValueCount(), equalTo(b.valueCount()));
        for (int p = 0; p < positionCount; p++) {
            BlockTestUtils.assertPositionValues(b.block(), p, equalTo(b.values().get(p)));
        }

        assertThat(b.block().mayHaveMultivaluedFields(), equalTo(b.values().stream().anyMatch(l -> l != null && l.size() > 1)));
    }

    public void testExpand() {
        int positionCount = randomIntBetween(1, 16 * 1024);
        var b = BasicBlockTests.randomBlock(elementType, positionCount, nullAllowed, 0, 100, 0, 0);
        assertExpanded(b.block());
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

    public void testFilteredNoopThenExpanded() {
        assertFilteredThenExpanded(true, false);
    }

    public void testFilteredReorderedThenExpanded() {
        assertFilteredThenExpanded(true, true);
    }

    public void testFilteredSubsetThenExpanded() {
        assertFilteredThenExpanded(false, false);
    }

    public void testFilteredJumbledSubsetThenExpanded() {
        assertFilteredThenExpanded(false, true);
    }

    private void assertFiltered(boolean all, boolean shuffled) {
        int positionCount = randomIntBetween(1, 16 * 1024);
        var b = BasicBlockTests.randomBlock(elementType, positionCount, nullAllowed, 0, 10, 0, 0);
        int[] positions = randomFilterPositions(b.block(), all, shuffled);
        Block filtered = b.block().filter(positions);

        assertThat(filtered.getPositionCount(), equalTo(positions.length));

        int expectedValueCount = 0;
        for (int p : positions) {
            List<Object> values = b.values().get(p);
            if (values != null) {
                expectedValueCount += values.size();
            }
        }
        assertThat(filtered.getTotalValueCount(), equalTo(expectedValueCount));
        for (int r = 0; r < positions.length; r++) {
            if (b.values().get(positions[r]) == null) {
                assertThat(filtered.getValueCount(r), equalTo(0));
                assertThat(filtered.isNull(r), equalTo(true));
            } else {
                assertThat(filtered.getValueCount(r), equalTo(b.values().get(positions[r]).size()));
                assertThat(BasicBlockTests.valuesAtPositions(filtered, r, r + 1).get(0), equalTo(b.values().get(positions[r])));
            }
        }

        assertThat(b.block().mayHaveMultivaluedFields(), equalTo(b.values().stream().anyMatch(l -> l != null && l.size() > 1)));
    }

    private int[] randomFilterPositions(Block orig, boolean all, boolean shuffled) {
        int[] positions = IntStream.range(0, orig.getPositionCount()).toArray();
        if (shuffled) {
            Randomness.shuffle(Arrays.asList(positions));
        }
        if (all) {
            return positions;
        }
        return IntStream.range(0, between(1, orig.getPositionCount())).map(i -> positions[i]).toArray();
    }

    private void assertExpanded(Block orig) {
        Block expanded = orig.expand();
        assertThat(expanded.getPositionCount(), equalTo(orig.getTotalValueCount() + orig.nullValuesCount()));
        assertThat(expanded.getTotalValueCount(), equalTo(orig.getTotalValueCount()));

        int np = 0;
        for (int op = 0; op < orig.getPositionCount(); op++) {
            if (orig.isNull(op)) {
                assertThat(expanded.isNull(np), equalTo(true));
                assertThat(expanded.getValueCount(np++), equalTo(0));
                continue;
            }
            List<Object> oValues = BasicBlockTests.valuesAtPositions(orig, op, op + 1).get(0);
            for (Object ov : oValues) {
                assertThat(expanded.isNull(np), equalTo(false));
                assertThat(expanded.getValueCount(np), equalTo(1));
                assertThat(BasicBlockTests.valuesAtPositions(expanded, np, ++np).get(0), equalTo(List.of(ov)));
            }
        }
    }

    private void assertFilteredThenExpanded(boolean all, boolean shuffled) {
        int positionCount = randomIntBetween(1, 16 * 1024);
        var b = BasicBlockTests.randomBlock(elementType, positionCount, nullAllowed, 0, 10, 0, 0);
        int[] positions = randomFilterPositions(b.block(), all, shuffled);
        assertExpanded(b.block().filter(positions));
    }
}
