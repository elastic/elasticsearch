/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.glob;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;

public class ListingExtentsTests extends ESTestCase {

    public void testUnboundedBoundsNeither() {
        assertFalse(ListingExtents.UNBOUNDED.boundsFileSet());
    }

    public void testAnExtentMustBeAtLeastOne() {
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> new ListingExtents(0)).getMessage(),
            containsString("maxFiles must be positive")
        );
    }

    public void testBoundsFileSetIsTrueOnlyWhenTheFileSetIsCapped() {
        assertTrue(new ListingExtents(10).boundsFileSet());
        assertFalse(ListingExtents.UNBOUNDED.boundsFileSet());
    }

    /**
     * The key ceiling reads this rather than {@link ListingExtents#boundsFileSet()}: a prefix-accepting listing
     * keeps its survivors there and reports itself truncated, and every other one stops hard. The two agree
     * today, so this pins the agreement rather than assuming it survives a change to either. Asserted as the
     * relation over the range: two chosen points pass a divergence that moves the threshold rather than removing
     * it.
     */
    public void testAskingForABoundIsWhatAcceptsAPrefix() {
        for (int maxFiles : new int[] { 1, 2, 5, randomIntBetween(1, 100), Integer.MAX_VALUE - 1, Integer.MAX_VALUE }) {
            ListingExtents extents = new ListingExtents(maxFiles);
            assertEquals("maxFiles=" + maxFiles, extents.boundsFileSet(), extents.acceptsPrefix());
        }
        assertTrue(new ListingExtents(10).acceptsPrefix());
        assertFalse(ListingExtents.UNBOUNDED.acceptsPrefix());
    }

}
