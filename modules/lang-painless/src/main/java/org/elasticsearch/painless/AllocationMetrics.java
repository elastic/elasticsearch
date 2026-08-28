/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless;

import org.elasticsearch.telemetry.metric.LongAsyncCounter;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Counts how much each script execution allocated, bucketed by powers of two, so an operator can see the real
 * distribution before committing to a limit.
 * <p>
 * A counter per bucket rather than a histogram instrument. A histogram takes a lock per record and builds an
 * {@code Attributes} from the attribute map, both once per execution; scripts run per document, so that serializes
 * concurrent searches. Counters aggregate through a striped adder, and putting the context and the size range in the
 * metric name means the record path passes no attributes at all.
 */
public final class AllocationMetrics {

    /** Smallest bucket boundary, 1kb. Executions below it land in the underflow bucket. */
    static final int MIN_BUCKET_EXPONENT = 10;

    /** Largest bucket boundary, 16gb. Executions at or above it all land in the top bucket. */
    static final int MAX_BUCKET_EXPONENT = 34;

    /** One bucket per boundary, plus the underflow bucket below the smallest. */
    static final int BUCKET_COUNT = MAX_BUCKET_EXPONENT - MIN_BUCKET_EXPONENT + 2;

    static final String METRIC_PREFIX = "es.script.painless.allocation.execution.";

    /** Stands in until real telemetry is installed. */
    public static final AllocationMetrics NOOP = new AllocationMetrics(MeterRegistry.NOOP);

    private final MeterRegistry meterRegistry;

    /** One recorder per context: its counters are registered once, and registering a name twice is an error. */
    private final Map<String, ContextRecorder> recordersByContext = new ConcurrentHashMap<>();

    public AllocationMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    /** The recorder for scripts of one context, built on first use and shared by every script compiled for it. */
    public ContextRecorder forContext(String scriptContextName) {
        return recordersByContext.computeIfAbsent(scriptContextName, this::newContextRecorder);
    }

    private ContextRecorder newContextRecorder(String scriptContextName) {
        ContextRecorder recorder = new ContextRecorder();
        LongAsyncCounter[] buckets = new LongAsyncCounter[BUCKET_COUNT];

        for (int bucket = 0; bucket < BUCKET_COUNT; ++bucket) {
            final int observed = bucket;
            buckets[bucket] = meterRegistry.registerLongAsyncCounter(
                metricName(scriptContextName, bucket),
                "Painless script executions allocating " + bucketDescription(bucket),
                "count",
                () -> new LongWithAttributes(recorder.count(observed))
            );
        }

        recorder.retain(buckets);

        return recorder;
    }

    /**
     * The bucket an execution's total falls in: 0 below {@link #MIN_BUCKET_EXPONENT}, then one per power of two, with
     * everything at or above {@link #MAX_BUCKET_EXPONENT} in the last. Derived from the leading-zero count rather than
     * searched for, since the boundaries are powers of two and this sits on the execution path.
     */
    static int bucketIndex(long totalBytes) {
        if (totalBytes < (1L << MIN_BUCKET_EXPONENT)) {
            return 0;
        }

        int exponent = Long.SIZE - 1 - Long.numberOfLeadingZeros(totalBytes);

        return Math.min(BUCKET_COUNT - 1, exponent - MIN_BUCKET_EXPONENT + 1);
    }

    /** {@code es.script.painless.allocation.execution.<context>.<range>.total}. */
    static String metricName(String scriptContextName, int bucket) {
        return METRIC_PREFIX + metricSegment(scriptContextName) + "." + bucketRange(bucket) + ".total";
    }

    /** The size range a bucket covers, as a name segment: {@code under_1kb}, then {@code from_1kb} upwards. */
    static String bucketRange(int bucket) {
        return bucket == 0 ? "under_" + size(MIN_BUCKET_EXPONENT) : "from_" + size(MIN_BUCKET_EXPONENT + bucket - 1);
    }

    /**
     * {@code 2^exponent} as a name segment. Not {@code ByteSizeValue}, which renders an exact byte count as {@code 1024b}
     * rather than {@code 1kb}; the boundaries are always whole powers of two, so the unit follows from the exponent.
     */
    private static String size(int exponent) {
        if (exponent < 20) {
            return (1 << (exponent - 10)) + "kb";
        }

        return exponent < 30 ? (1 << (exponent - 20)) + "mb" : (1 << (exponent - 30)) + "gb";
    }

    private static String bucketDescription(int bucket) {
        if (bucket == 0) {
            return "less than " + size(MIN_BUCKET_EXPONENT);
        }

        if (bucket == BUCKET_COUNT - 1) {
            return size(MAX_BUCKET_EXPONENT) + " or more";
        }

        return "at least " + size(MIN_BUCKET_EXPONENT + bucket - 1) + " and less than " + size(MIN_BUCKET_EXPONENT + bucket);
    }

    /**
     * Context names reach the metric name, which only permits {@code [a-z][a-z0-9_]*} segments. Every context in the
     * tree already conforms, so this only guards a plugin-defined name from failing metric registration at compile time.
     */
    private static String metricSegment(String scriptContextName) {
        StringBuilder segment = new StringBuilder(scriptContextName.length());

        for (char character : scriptContextName.toLowerCase(Locale.ROOT).toCharArray()) {
            segment.append((character >= 'a' && character <= 'z') || (character >= '0' && character <= '9') ? character : '_');
        }

        if (segment.isEmpty() || segment.charAt(0) < 'a' || segment.charAt(0) > 'z') {
            segment.insert(0, 'c');
        }

        return segment.substring(0, Math.min(segment.length(), 30));
    }

    /**
     * Counts executions of one script context, in per-thread rows that the counters read at collection time.
     * <p>
     * The counters are asynchronous, so an execution touches no instrument at all: it increments one slot of the row its
     * thread is striped onto, and the observers sum each column when telemetry collects. Nothing is pushed and nothing has
     * to be flushed, which matters because a script instance has no end-of-life hook to flush on.
     */
    public static final class ContextRecorder {

        private static final VarHandle SLOT = MethodHandles.arrayElementVarHandle(long[].class);

        /** Enough rows that threads rarely share one; the write pool is sized to allocated processors. */
        private static final int STRIPES = Integer.highestOneBit(Math.max(8, Runtime.getRuntime().availableProcessors() * 2 - 1)) * 2;

        /** Separately allocated rows, so two threads never contend on the same cache line. */
        private final long[][] rows = new long[STRIPES][BUCKET_COUNT];

        /** Held only to keep the registered counters, and their observers, alive alongside this recorder. */
        private LongAsyncCounter[] buckets;

        private ContextRecorder() {}

        private void retain(LongAsyncCounter[] buckets) {
            this.buckets = buckets;
        }

        /**
         * Counts one execution's total, from the generated {@code execute} method's return path. Atomic rather than a plain
         * increment: rows are per-thread so the add is uncontended and the two measured the same, and this way two threads
         * sharing a row cannot lose a count.
         */
        public void recordExecutionAllocation(long totalBytes) {
            long[] row = rows[(int) (Thread.currentThread().threadId() & (STRIPES - 1))];

            SLOT.getAndAdd(row, bucketIndex(totalBytes), 1L);
        }

        /** The executions counted in one bucket so far, summed across rows. Called when telemetry collects. */
        long count(int bucket) {
            long total = 0;

            for (long[] row : rows) {
                total += (long) SLOT.getVolatile(row, bucket);
            }

            return total;
        }
    }
}
