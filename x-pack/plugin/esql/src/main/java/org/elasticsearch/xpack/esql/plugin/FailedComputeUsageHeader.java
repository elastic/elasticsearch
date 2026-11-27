/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.scheduling.SearchLoadMetrics;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongBinaryOperator;

/**
 * Carries what a data node did for a compute that failed back to the coordinator, as a transport response header.
 * <p>
 * The failure of a data node request is an exception, which cannot carry usage: wrapping it would change what the failure
 * handling sees, and metadata added to it ends up in the error the client gets. A response header travels with an error response
 * as it does with a successful one, needs no new transport version, and the receiving side can {@link #take take} it off the
 * thread context, so it never reaches the client.
 * <p>
 * A coordinator that does not know the header does not take it, and passes it on to the client with the error. That can only
 * happen while a cluster is being upgraded, and the header holds nothing but counters, so it is accepted.
 * <p>
 * Only numbers are carried, as one value of comma separated longs behind a format version. A value that cannot be read is
 * ignored: the usage is best effort and must never turn into a failure of its own. The search load goes in a header of its own,
 * {@link #SEARCH_LOAD_NAME}, so that a coordinator that reads only the other counters still reads them, and so does the peak
 * memory, {@link #PEAK_MEMORY_NAME}. Several values of a header add up, except for the peak memory, which takes the highest.
 */
final class FailedComputeUsageHeader {
    private static final Logger logger = LogManager.getLogger(FailedComputeUsageHeader.class);

    static final String NAME = "X-Elasticsearch-Esql-Failed-Compute-Usage";
    static final String SEARCH_LOAD_NAME = "X-Elasticsearch-Esql-Failed-Compute-Search-Load";
    static final String PEAK_MEMORY_NAME = "X-Elasticsearch-Esql-Failed-Compute-Peak-Memory";
    private static final String FORMAT = "1:";
    /**
     * The counters of {@link DriverCompletionInfo} that add up, other than the search load and the peak memory: see {@link #write}
     * and {@link #take}.
     */
    static final int FIELDS = 8;
    /** The search load counters of {@link DriverCompletionInfo}: see {@link #write} and {@link #take}. */
    static final int SEARCH_LOAD_FIELDS = 3;

    private FailedComputeUsageHeader() {}

    /**
     * Adds {@code usage} to the response headers of the current context, to be sent with the response that is sent from it. Does
     * nothing for the counters that are all zero.
     */
    static void write(ThreadContext threadContext, DriverCompletionInfo usage) {
        write(
            threadContext,
            NAME,
            usage.documentsFound(),
            usage.valuesLoaded(),
            usage.rowsEmitted(),
            usage.bytesRead(),
            usage.readNanos(),
            usage.readCpuNanos(),
            usage.cpuNanos(),
            usage.driverNanos()
        );
        SearchLoadMetrics searchLoad = usage.searchLoad();
        write(threadContext, SEARCH_LOAD_NAME, searchLoad.loadNanos(), searchLoad.cpuNanos(), searchLoad.waitingNanos());
        write(threadContext, PEAK_MEMORY_NAME, usage.peakMemoryBytes());
    }

    private static void write(ThreadContext threadContext, String name, long... fields) {
        boolean empty = true;
        StringBuilder value = new StringBuilder(FORMAT);
        for (int i = 0; i < fields.length; i++) {
            empty &= fields[i] == 0;
            value.append(i == 0 ? "" : ",").append(fields[i]);
        }
        if (empty == false) {
            threadContext.addResponseHeader(name, value.toString());
        }
    }

    /**
     * Removes the headers from the response headers of the current context and returns what they carried, combined if there are
     * several, or {@link DriverCompletionInfo#EMPTY} if there is none. Always removes them, also when they cannot be read.
     */
    static DriverCompletionInfo take(ThreadContext threadContext) {
        long[] sum = take(threadContext, NAME, FIELDS, Long::sum);
        long[] searchLoad = take(threadContext, SEARCH_LOAD_NAME, SEARCH_LOAD_FIELDS, Long::sum);
        long[] peakMemory = take(threadContext, PEAK_MEMORY_NAME, 1, Math::max);
        if (sum == null && searchLoad == null && peakMemory == null) {
            return DriverCompletionInfo.EMPTY;
        }
        sum = sum == null ? new long[FIELDS] : sum;
        searchLoad = searchLoad == null ? new long[SEARCH_LOAD_FIELDS] : searchLoad;
        peakMemory = peakMemory == null ? new long[1] : peakMemory;
        return new DriverCompletionInfo(
            sum[0],
            sum[1],
            sum[2],
            sum[3],
            sum[4],
            sum[5],
            sum[6],
            sum[7],
            new SearchLoadMetrics(searchLoad[0], searchLoad[1], searchLoad[2]),
            peakMemory[0],
            List.of(),
            List.of(),
            Map.of(),
            false,
            false,
            Set.of()
        );
    }

    /**
     * The values of the header {@code name} that can be read, combined field by field with {@code combine}, or {@code null} if none
     * can.
     */
    private static long[] take(ThreadContext threadContext, String name, int fieldCount, LongBinaryOperator combine) {
        List<String> values = threadContext.takeResponseHeaders(name);
        long[] combined = null;
        for (String value : values) {
            long[] fields = parse(value, fieldCount);
            if (fields == null) {
                continue;
            }
            if (combined == null) {
                combined = fields;
            } else {
                for (int i = 0; i < fieldCount; i++) {
                    combined[i] = combine.applyAsLong(combined[i], fields[i]);
                }
            }
        }
        return combined;
    }

    private static long[] parse(String value, int fieldCount) {
        if (value.startsWith(FORMAT) == false) {
            logger.debug("ignoring the usage of a failed compute in an unknown format [{}]", value);
            return null;
        }
        String[] parts = value.substring(FORMAT.length()).split(",");
        if (parts.length != fieldCount) {
            logger.debug("ignoring the usage of a failed compute with [{}] fields instead of [{}]", parts.length, fieldCount);
            return null;
        }
        long[] fields = new long[fieldCount];
        try {
            for (int i = 0; i < fieldCount; i++) {
                fields[i] = Long.parseLong(parts[i]);
            }
        } catch (NumberFormatException e) {
            logger.debug("ignoring the usage of a failed compute that is not made of numbers [{}]", value);
            return null;
        }
        return fields;
    }
}
