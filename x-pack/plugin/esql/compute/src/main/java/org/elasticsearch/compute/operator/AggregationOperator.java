/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.aggregation.Aggregator;
import org.elasticsearch.compute.aggregation.Aggregator.Factory;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

/**
 * Blocking aggregation operator.  An aggregation operator aggregates its input with one or more
 * aggregator functions, e.g. avg, max, etc, and outputs a Page containing the results of those
 * aggregations.
 *
 * The operator is blocking in the sense that it only produces output once all possible input has
 * been added, that is, when the {@link #finish} method has been called.
 */
public class AggregationOperator implements Operator {

    private boolean finished;
    private Page output;
    private final List<Aggregator> aggregators;
    private final DriverContext driverContext;

    /**
     * Nanoseconds this operator has spent running the aggregations.
     */
    private long aggregationNanos;
    /**
     * Nanoseconds this operator has spent running the aggregations final evaluation.
     */
    private long aggregationFinishNanos;
    /**
     * Count of pages this operator has processed.
     */
    private int pagesProcessed;
    /**
     * Count of rows this operator has received.
     */
    private long rowsReceived;
    /**
     * Count of rows this operator has emitted.
     */
    private long rowsEmitted;

    public record AggregationOperatorFactory(List<Factory> aggregators, AggregatorMode mode) implements OperatorFactory {

        @Override
        public Operator get(DriverContext driverContext) {
            return new AggregationOperator(aggregators.stream().map(x -> x.apply(driverContext)).toList(), driverContext);
        }

        @Override
        public String toString() {
            return describe();
        }

        @Override
        public String describe() {
            return "AggregationOperator[mode = "
                + mode
                + ", aggs = "
                + aggregators.stream().map(Factory::describe).collect(joining(", "))
                + "]";
        }
    }

    public AggregationOperator(List<Aggregator> aggregators, DriverContext driverContext) {
        Objects.requireNonNull(aggregators);
        checkNonEmpty(aggregators);
        this.aggregators = aggregators;
        this.driverContext = driverContext;
    }

    @Override
    public boolean needsInput() {
        return finished == false;
    }

    @Override
    public void addInput(Page page) {
        long start = System.nanoTime();
        checkState(needsInput(), "Operator is already finishing");
        requireNonNull(page, "page is null");
        try (BooleanVector noMasking = driverContext.blockFactory().newConstantBooleanVector(true, page.getPositionCount())) {
            for (Aggregator aggregator : aggregators) {
                aggregator.processPage(page, noMasking);
            }
        } finally {
            page.releaseBlocks();
            aggregationNanos += System.nanoTime() - start;
            pagesProcessed++;
            rowsReceived += page.getPositionCount();
        }
    }

    @Override
    public Page getOutput() {
        Page p = output;
        if (p != null) {
            rowsEmitted += p.getPositionCount();
        }
        this.output = null;
        return p;
    }

    @Override
    public void finish() {
        if (finished) {
            return;
        }
        long start = System.nanoTime();
        finished = true;
        Block[] blocks = null;
        boolean success = false;
        try {
            int[] aggBlockCounts = aggregators.stream().mapToInt(Aggregator::evaluateBlockCount).toArray();
            // TODO: look into allocating the blocks lazily
            blocks = new Block[Arrays.stream(aggBlockCounts).sum()];
            int offset = 0;
            for (int i = 0; i < aggregators.size(); i++) {
                var aggregator = aggregators.get(i);
                aggregator.evaluate(blocks, offset, driverContext);
                offset += aggBlockCounts[i];
            }
            output = new Page(blocks);
            success = true;
        } finally {
            if (success == false && blocks != null) {
                Releasables.closeExpectNoException(blocks);
            }
            aggregationFinishNanos += System.nanoTime() - start;
        }
    }

    @Override
    public boolean isFinished() {
        return finished && output == null;
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(() -> {
            if (output != null) {
                Releasables.closeExpectNoException(() -> output.releaseBlocks());
            }
        }, Releasables.wrap(aggregators));
    }

    private static void checkState(boolean condition, String msg) {
        if (condition == false) {
            throw new IllegalArgumentException(msg);
        }
    }

    private static void checkNonEmpty(List<?> list) {
        if (list.size() < 1) {
            throw new IllegalArgumentException("empty list");
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass().getSimpleName()).append("[");
        sb.append("aggregators=").append(aggregators).append("]");
        return sb.toString();
    }

    @Override
    public Operator.Status status() {
        return new Status(aggregationNanos, aggregationFinishNanos, pagesProcessed, rowsReceived, rowsEmitted);
    }

    public static class Status implements Operator.Status {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "agg",
            Status::new
        );

        /**
         * Nanoseconds this operator has spent running the aggregations.
         */
        private final long aggregationNanos;

        /**
         * Nanoseconds this operator has spent running the aggregations final evaluation.
         */
        private final Long aggregationFinishNanos;
        /**
         * Count of pages this operator has processed.
         */
        private final int pagesProcessed;
        /**
         * Count of rows this operator has received.
         */
        private final long rowsReceived;
        /**
         * Count of rows this operator has emitted.
         */
        private final long rowsEmitted;

        /**
         * Build.
         * @param aggregationNanos Nanoseconds this operator has spent running the aggregations.
         * @param aggregationFinishNanos Nanoseconds this operator has spent running the aggregations.
         * @param pagesProcessed Count of pages this operator has processed.
         */
        public Status(long aggregationNanos, long aggregationFinishNanos, int pagesProcessed, long rowsReceived, long rowsEmitted) {
            this.aggregationNanos = aggregationNanos;
            this.aggregationFinishNanos = aggregationFinishNanos;
            this.pagesProcessed = pagesProcessed;
            this.rowsReceived = rowsReceived;
            this.rowsEmitted = rowsEmitted;
        }

        protected Status(StreamInput in) throws IOException {
            aggregationNanos = in.readVLong();
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
                aggregationFinishNanos = in.readOptionalVLong();
            } else {
                aggregationFinishNanos = null;
            }
            pagesProcessed = in.readVInt();
            if (in.getTransportVersion().onOrAfter(TransportVersions.ESQL_PROFILE_ROWS_PROCESSED)) {
                rowsReceived = in.readVLong();
                rowsEmitted = in.readVLong();
            } else {
                rowsReceived = 0;
                rowsEmitted = 0;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(aggregationNanos);
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
                out.writeOptionalVLong(aggregationFinishNanos);
            }
            out.writeVInt(pagesProcessed);
            if (out.getTransportVersion().onOrAfter(TransportVersions.ESQL_PROFILE_ROWS_PROCESSED)) {
                out.writeVLong(rowsReceived);
                out.writeVLong(rowsEmitted);
            }
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        /**
         * Nanoseconds this operator has spent running the aggregations.
         */
        public long aggregationNanos() {
            return aggregationNanos;
        }

        /**
         * Nanoseconds this operator has spent running the aggregations final evaluation.
         */
        public long aggregationFinishNanos() {
            return aggregationFinishNanos;
        }

        /**
         * Count of pages this operator has processed.
         */
        public int pagesProcessed() {
            return pagesProcessed;
        }

        /**
         * Count of rows this operator has received.
         */
        public long rowsReceived() {
            return rowsReceived;
        }

        /**
         * Count of rows this operator has emitted.
         */
        public long rowsEmitted() {
            return rowsEmitted;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("aggregation_nanos", aggregationNanos);
            if (builder.humanReadable()) {
                builder.field("aggregation_time", TimeValue.timeValueNanos(aggregationNanos));
            }
            builder.field("aggregation_finish_nanos", aggregationFinishNanos);
            if (builder.humanReadable()) {
                builder.field(
                    "aggregation_finish_time",
                    aggregationFinishNanos == null ? null : TimeValue.timeValueNanos(aggregationFinishNanos)
                );
            }
            builder.field("pages_processed", pagesProcessed);
            builder.field("rows_received", rowsReceived);
            builder.field("rows_emitted", rowsEmitted);
            return builder.endObject();

        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Status status = (Status) o;
            return aggregationNanos == status.aggregationNanos
                && Objects.equals(aggregationFinishNanos, status.aggregationFinishNanos)
                && pagesProcessed == status.pagesProcessed
                && rowsReceived == status.rowsReceived
                && rowsEmitted == status.rowsEmitted;
        }

        @Override
        public int hashCode() {
            return Objects.hash(aggregationNanos, aggregationFinishNanos, pagesProcessed, rowsReceived, rowsEmitted);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersions.V_8_14_0;
        }
    }
}
