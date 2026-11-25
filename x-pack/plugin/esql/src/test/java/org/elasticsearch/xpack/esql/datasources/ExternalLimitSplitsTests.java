/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;

public class ExternalLimitSplitsTests extends ESTestCase {

    public void testSingleDriverLimitsNeedNoCuts() {
        int page = ExternalLimitSplits.DEFAULT_PAGE_SIZE_ROWS;
        int concurrency = 19;
        long stride = 64L << 20;
        long rowBytes = ExternalLimitSplits.DEFAULT_ROW_BYTES;
        for (int limit : new int[] { 10, 1000 }) {
            assertEquals(1, ExternalLimitSplits.driverCount(limit, page, concurrency));
            assertEquals(1, ExternalLimitSplits.targetSplits(limit, page, concurrency, stride, rowBytes));
            assertEquals(0, ExternalLimitSplits.demandCuts(limit, page, concurrency, 1, stride, rowBytes));
        }
    }

    public void testFivePagesPerDriverMatchesPlanner() {
        int page = ExternalLimitSplits.DEFAULT_PAGE_SIZE_ROWS;
        assertEquals(5, ExternalLimitSplits.MIN_PAGES_PER_LIMIT_DRIVER);
        assertEquals(2, ExternalLimitSplits.driverCount(10_000, page, 19));
        assertEquals(1, ExternalLimitSplits.driverCount(3_000, page, 19));
        assertEquals(4, ExternalLimitSplits.driverCount(10_000, 500, 19));
        assertEquals(2, ExternalLimitSplits.driverCount(6_000, page, 19));
        assertEquals(3, ExternalLimitSplits.driverCount(11_000, page, 19));
    }

    public void testHundredThousandCutsToStrideNotDrivers() {
        int page = ExternalLimitSplits.DEFAULT_PAGE_SIZE_ROWS;
        int concurrency = 19;
        long stride = 64L << 20;
        long rowBytes = ExternalLimitSplits.DEFAULT_ROW_BYTES;
        assertEquals(19, ExternalLimitSplits.driverCount(100_000, page, concurrency));
        assertEquals(7, ExternalLimitSplits.targetSplits(100_000, page, concurrency, stride, rowBytes));
        assertEquals(6, ExternalLimitSplits.demandCuts(100_000, page, concurrency, 1, stride, rowBytes));
        assertEquals(0, ExternalLimitSplits.demandCuts(100_000, page, concurrency, 8, stride, rowBytes));
    }

    public void testTaskConcurrencyOneIsAlwaysOneDriver() {
        int page = ExternalLimitSplits.DEFAULT_PAGE_SIZE_ROWS;
        long stride = 64L << 20;
        long rowBytes = ExternalLimitSplits.DEFAULT_ROW_BYTES;
        for (int limit : new int[] { 10, 1000, 100_000 }) {
            assertEquals(1, ExternalLimitSplits.driverCount(limit, page, 1));
            assertEquals(0, ExternalLimitSplits.demandCuts(limit, page, 1, 1, stride, rowBytes));
        }
    }

    public void testNoLimitIsUnboundedCuts() {
        assertEquals(Integer.MAX_VALUE, ExternalLimitSplits.demandCuts(FormatReader.NO_LIMIT, 1000, 19, 1, 64L << 20, 4096));
    }

    public void testDriverCountRejectsNoLimit() {
        expectThrows(IllegalArgumentException.class, () -> ExternalLimitSplits.driverCount(FormatReader.NO_LIMIT, 1000, 19));
    }
}
