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
     * Count of pages this operator has processed.
     */
    private int pagesProcessed;

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
        try {
            for (Aggregator aggregator : aggregators) {
                aggregator.processPage(page);
            }
        } finally {
            page.releaseBlocks();
            aggregationNanos += System.nanoTime() - start;
            pagesProcessed++;
        }
    }

    @Override
    public Page getOutput() {
        Page p = output;
        this.output = null;
        return p;
    }

    @Override
    public void finish() {
        if (finished) {
            return;
        }
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
        return new Status(aggregationNanos, pagesProcessed);
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
         * Count of pages this operator has processed.
         */
        private final int pagesProcessed;

        /**
         * Build.
         * @param aggregationNanos Nanoseconds this operator has spent running the aggregations.
         * @param pagesProcessed Count of pages this operator has processed.
         */
        public Status(long aggregationNanos, int pagesProcessed) {
            this.aggregationNanos = aggregationNanos;
            this.pagesProcessed = pagesProcessed;
        }

        protected Status(StreamInput in) throws IOException {
            aggregationNanos = in.readVLong();
            pagesProcessed = in.readVInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(aggregationNanos);
            out.writeVInt(pagesProcessed);
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
         * Count of pages this operator has processed.
         */
        public int pagesProcessed() {
            return pagesProcessed;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("aggregation_nanos", aggregationNanos);
            if (builder.humanReadable()) {
                builder.field("aggregation_time", TimeValue.timeValueNanos(aggregationNanos));
            }
            builder.field("pages_processed", pagesProcessed);
            return builder.endObject();

        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Status status = (Status) o;
            return aggregationNanos == status.aggregationNanos && pagesProcessed == status.pagesProcessed;
        }

        @Override
        public int hashCode() {
            return Objects.hash(aggregationNanos, pagesProcessed);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersions.ESQL_TIMINGS;
        }
    }
}
