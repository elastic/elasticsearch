/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;

/**
 * How many record-aligned cuts an unfiltered LIMIT is worth, and how many drivers will run them.
 * {@code LocalExecutionPlanner} and {@link FileSplitProvider} share this so a single-driver LIMIT
 * does not probe a wave of unused splits.
 */
public final class ExternalLimitSplits {

    /**
     * Default page size when the exec has no row-size estimate. Same number
     * {@code LocalExecutionPlanner.DEFAULT_EXTERNAL_SOURCE_PAGE_SIZE_ROWS} uses.
     */
    public static final int DEFAULT_PAGE_SIZE_ROWS = 1000;

    /** Bytes per row when schema inference did not publish a sample width. */
    public static final int DEFAULT_ROW_BYTES = 4096;

    /**
     * Query-pragma default for {@code task_concurrency}: the search thread-pool size on an empty
     * settings object. Discovery uses this when the caller did not pass a pragma value.
     */
    public static final int DEFAULT_TASK_CONCURRENCY = ThreadPool.searchOrGetThreadPoolSize(
        EsExecutors.allocatedProcessors(Settings.EMPTY)
    );

    /**
     * Minimum pages of work each pushed-LIMIT driver must have. Same number
     * {@code LocalExecutionPlanner.limitDriverCount} uses so discovery cuts match
     * the drivers that will run.
     */
    public static final int MIN_PAGES_PER_LIMIT_DRIVER = 5;

    private ExternalLimitSplits() {}

    /**
     * Drivers the planner starts for an unfiltered pushed limit, before {@code min(splitCount)}.
     * {@code LIMIT <= pageSize} is always one driver. Larger demand needs
     * {@link #MIN_PAGES_PER_LIMIT_DRIVER} pages per driver. {@link FormatReader#NO_LIMIT} is not a
     * demand: the caller must not use this to size a full scan.
     */
    public static int driverCount(int rowLimit, int pageSize, int taskConcurrency) {
        if (rowLimit == FormatReader.NO_LIMIT || rowLimit <= 0) {
            throw new IllegalArgumentException("driverCount is only defined under a positive row demand");
        }
        int page = pageSize > 0 ? pageSize : DEFAULT_PAGE_SIZE_ROWS;
        int concurrency = Math.max(1, taskConcurrency);
        if (rowLimit <= page) {
            return 1;
        }
        int fromBudget = (int) Math.ceilDiv((long) rowLimit, (long) MIN_PAGES_PER_LIMIT_DRIVER * page);
        return Math.min(Math.max(fromBudget, 1), concurrency);
    }

    /**
     * Splits worth cutting under demand: {@code min(drivers, ceil(rowLimit * rowBytes / stride))}.
     * Whole files already give that many starting points, so {@link #demandCuts} subtracts them.
     */
    public static int targetSplits(int rowLimit, int pageSize, int taskConcurrency, long strideBytes, long rowBytes) {
        int drivers = driverCount(rowLimit, pageSize, taskConcurrency);
        long bytes = Math.multiplyExact((long) rowLimit, Math.max(1L, rowBytes));
        long stride = strideBytes > 0 ? strideBytes : 1L;
        long strideFits = Math.max(1L, Math.ceilDiv(bytes, stride));
        return (int) Math.min(drivers, Math.min(strideFits, Integer.MAX_VALUE));
    }

    /**
     * Stride cuts (not including file start 0) a demand-limited scan may issue, counting each
     * listed file as a starting point. Zero means every file stays whole-file.
     */
    public static int demandCuts(int rowLimit, int pageSize, int taskConcurrency, int fileCount, long strideBytes, long rowBytes) {
        if (rowLimit == FormatReader.NO_LIMIT || rowLimit <= 0) {
            return Integer.MAX_VALUE;
        }
        int target = targetSplits(rowLimit, pageSize, taskConcurrency, strideBytes, rowBytes);
        return Math.max(0, target - Math.max(0, fileCount));
    }
}
