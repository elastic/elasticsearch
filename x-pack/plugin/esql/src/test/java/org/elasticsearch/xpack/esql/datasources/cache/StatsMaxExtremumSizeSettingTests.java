/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;

import static org.hamcrest.Matchers.containsString;

/**
 * The setting's bounds. The ceiling must convert with {@code Math.toIntExact}, which is how
 * {@code FileSourceFactory} reads it.
 */
public class StatsMaxExtremumSizeSettingTests extends ESTestCase {

    public void testTheDefaultIsTheWidthAReadFallsBackTo() {
        assertEquals(
            FormatReadContext.DEFAULT_MAX_EXTREMUM_BYTES,
            Math.toIntExact(ExternalSourceCacheSettings.STATS_MAX_EXTREMUM_SIZE.get(Settings.EMPTY).getBytes())
        );
    }

    public void testZeroIsInsideTheRange() {
        assertEquals(0L, get("0b").getBytes());
    }

    public void testTheCeilingIsAMegabyteAndConvertsToAnInt() {
        assertEquals(ByteSizeValue.ofMb(1), get("1mb"));
        assertEquals(1024 * 1024, Math.toIntExact(get("1mb").getBytes()));
    }

    public void testAWidthAboveTheCeilingIsRejected() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> get("2mb"));
        assertThat(e.getMessage(), containsString("esql.external.cache.stats.max_extremum_size"));
    }

    public void testANegativeWidthIsRejected() {
        expectThrows(IllegalArgumentException.class, () -> get("-1b"));
    }

    private static ByteSizeValue get(String value) {
        return ExternalSourceCacheSettings.STATS_MAX_EXTREMUM_SIZE.get(
            Settings.builder().put("esql.external.cache.stats.max_extremum_size", value).build()
        );
    }
}
