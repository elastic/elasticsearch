/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.unit;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class MemorySizeValueTests extends ESTestCase {

    // -----------------------------------------------------------------
    // maxDirectMemory()
    // -----------------------------------------------------------------

    public void testMaxDirectMemoryIsPositive() {
        // The fallback ensures this is always > 0 regardless of JVM flags.
        assertThat(MemorySizeValue.maxDirectMemory(), greaterThan(0L));
    }

    // -----------------------------------------------------------------
    // parseBytesSizeValueOrDirectMemoryRatio — percentage forms
    // Percentages resolve against NativeMemoryLimitCalculator.nativeMemoryBase(),
    // which is cgroup-aware; on non-container JVMs it equals maxDirectMemory().
    // -----------------------------------------------------------------

    public void testFiftyPercentResolvesAgainstNativeMemoryBase() {
        final long expected = NativeMemoryLimitCalculator.nativeMemoryBase() / 2;
        final ByteSizeValue result = MemorySizeValue.parseBytesSizeValueOrDirectMemoryRatio("50%", "setting.name");
        assertThat(result.getBytes(), equalTo(expected));
    }

    public void testZeroPercentThrows() {
        expectThrows(ElasticsearchParseException.class, () -> MemorySizeValue.parseBytesSizeValueOrDirectMemoryRatio("0%", "setting.name"));
    }

    public void testHundredPercentReturnsFullNativeMemoryBase() {
        final long expected = NativeMemoryLimitCalculator.nativeMemoryBase();
        final ByteSizeValue result = MemorySizeValue.parseBytesSizeValueOrDirectMemoryRatio("100%", "setting.name");
        assertThat(result.getBytes(), equalTo(expected));
    }

    public void testOverHundredPercentThrows() {
        expectThrows(
            ElasticsearchParseException.class,
            () -> MemorySizeValue.parseBytesSizeValueOrDirectMemoryRatio("101%", "setting.name")
        );
    }

    public void testNegativePercentThrows() {
        expectThrows(
            ElasticsearchParseException.class,
            () -> MemorySizeValue.parseBytesSizeValueOrDirectMemoryRatio("-1%", "setting.name")
        );
    }

    public void testNonNumericPercentThrows() {
        expectThrows(
            ElasticsearchParseException.class,
            () -> MemorySizeValue.parseBytesSizeValueOrDirectMemoryRatio("abc%", "setting.name")
        );
    }

    // -----------------------------------------------------------------
    // parseBytesSizeValueOrDirectMemoryRatio — absolute forms
    // -----------------------------------------------------------------

    public void testAbsoluteByteValue() {
        final ByteSizeValue result = MemorySizeValue.parseBytesSizeValueOrDirectMemoryRatio("1024b", "setting.name");
        assertThat(result.getBytes(), equalTo(1024L));
    }

    public void testAbsoluteMegabyteValue() {
        final ByteSizeValue result = MemorySizeValue.parseBytesSizeValueOrDirectMemoryRatio("256mb", "setting.name");
        assertThat(result.getBytes(), equalTo(ByteSizeValue.of(256, ByteSizeUnit.MB).getBytes()));
    }
}
