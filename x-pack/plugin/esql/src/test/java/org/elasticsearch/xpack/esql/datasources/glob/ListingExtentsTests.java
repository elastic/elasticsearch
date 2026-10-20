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

}
