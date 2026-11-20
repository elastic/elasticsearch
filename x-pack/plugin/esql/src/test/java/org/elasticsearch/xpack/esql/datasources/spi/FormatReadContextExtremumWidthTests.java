/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.test.ESTestCase;

import java.util.List;

/**
 * The unset-versus-zero encoding on the extremum width. Zero is a real setting — withhold every text
 * extremum that has bytes — so "unset" cannot also be zero, and the normalisation has to turn only a
 * negative into the default. Widening that test to {@code <= 0} would make a deliberate zero silently
 * become a kilobyte, which no other test in the tree would notice.
 */
public class FormatReadContextExtremumWidthTests extends ESTestCase {

    public void testAnUnsetWidthTakesTheDefault() {
        assertEquals(
            FormatReadContext.DEFAULT_MAX_EXTREMUM_BYTES,
            FormatReadContext.builder().projectedColumns(List.of("a")).batchSize(1).build().statsMaxExtremumBytes()
        );
    }

    public void testANegativeWidthTakesTheDefault() {
        assertEquals(FormatReadContext.DEFAULT_MAX_EXTREMUM_BYTES, context(-1).statsMaxExtremumBytes());
        assertEquals(FormatReadContext.DEFAULT_MAX_EXTREMUM_BYTES, context(Integer.MIN_VALUE).statsMaxExtremumBytes());
    }

    public void testZeroSurvivesNormalisationBecauseItIsNotUnset() {
        assertEquals("zero must not be read as unset", 0, context(0).statsMaxExtremumBytes());
    }

    public void testAWidthIsCarriedThrough() {
        assertEquals(7, context(7).statsMaxExtremumBytes());
    }

    private static FormatReadContext context(int maxExtremumBytes) {
        return FormatReadContext.builder().projectedColumns(List.of("a")).batchSize(1).statsMaxExtremumBytes(maxExtremumBytes).build();
    }
}
