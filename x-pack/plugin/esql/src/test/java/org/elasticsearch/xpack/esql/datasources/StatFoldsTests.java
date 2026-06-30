/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;

/**
 * Pins the key→fold-law mapping the statistics combine dispatches through: extrema fold by min/max,
 * counts/sizes by sum, and a non-statistic key has no fold (the combine leaves it alone).
 */
public class StatFoldsTests extends ESTestCase {

    public void testExtremaFoldByMinMax() {
        assertSame(StatFold.MIN, StatFolds.foldFor("_stats.columns.value.min"));
        assertSame(StatFold.MAX, StatFolds.foldFor("_stats.columns.value.max"));
    }

    public void testCountsAndSizesFoldBySum() {
        assertSame(StatFold.SUM, StatFolds.foldFor("_stats.row_count"));
        assertSame(StatFold.SUM, StatFolds.foldFor("_stats.size_bytes"));
        assertSame(StatFold.SUM, StatFolds.foldFor("_stats.columns.value.null_count"));
        assertSame(StatFold.SUM, StatFolds.foldFor("_stats.columns.value.size_bytes"));
    }

    public void testNonStatisticKeyHasNoFold() {
        assertNull(StatFolds.foldFor("_mtime_millis"));
        assertNull(StatFolds.foldFor("config.fingerprint"));
        assertNull(StatFolds.foldFor("_stats.stripe.3"));
    }
}
