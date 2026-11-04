/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.aggregation.Aggregator;
import org.elasticsearch.compute.aggregation.Aggregator.Factory;
import org.elasticsearch.compute.aggregation.AggregatorFunction;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.FilteredAggregatorFunction;
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
 * Aggregates input {@link Page}s into a single output row.
 * <p>
 *     Receives input pages until we call {@link #finish()}, telling it
 *     there are no more pages. Each aggregation is an instance
 *     of {@link AggregatorFunction}.
 * </p>
 * <p>
 *     For
 *     {@snippet lang="esql" :
 *     | STATS MIN(discovered), MAX(discovered)
 *     }
 *     this'd look like:
 * </p>
 * {@snippet lang="txt" :
 * Before first page:
 *                                     ┌─────────────────┬─────────────────┐
 *                                     │ MIN(discovered) │ MAX(discovered) │
 *                                     ├─────────────────┼─────────────────┤
 *                                     │ null            │ null            │
 *                                     └─────────────────┴─────────────────┘
 *
 * First page:
 * ┌──────┬──────────┬────────────┐    ┌─────────────────┬─────────────────┐
 * │  ref │ class    │ discovered │    │ MIN(discovered) │ MAX(discovered) │
 * ├──────┼──────────┼────────────┤    ├─────────────────┼─────────────────┤
 * │  079 │ Euclid   │ 1988-01-01 │ -> │ 1967-01-01      │ 1993-01-01      │
 * │  173 │ Euclid   │ 1993-01-01 │    └─────────────────┴─────────────────┘
 * │ 1313 │ Keter    │ 1967-01-01 │
 * │ 1981 │ Safe     │ 1991-01-01 │
 * └──────┴──────────┴────────────┘
 *
 * Second page:
 * ┌──────┬──────────┬────────────┐    ┌─────────────────┬─────────────────┐
 * │  ref │ class    │ discovered │    │ MIN(discovered) │ MAX(discovered) │
 * ├──────┼──────────┼────────────┤    ├─────────────────┼─────────────────┤
 * │ 2317 │ Keter    │ 1922-01-01 │ -> │ 1922-01-01      │ 2016-04-28      │
 * │ 2639 │ Euclid   │ 2010-01-01 │    └─────────────────┴─────────────────┘
 * │ 2935 │ Keter    │ 2016-04-28 │
 * │ 3000 │ Thaumiel │ 1971-01-01 │
 * └──────┴──────────┴────────────┘
 *
 * Third page:
 * ┌──────┬──────────┬────────────┐    ┌─────────────────┬─────────────────┐
 * │  ref │ class    │ discovered │    │ MIN(discovered) │ MAX(discovered) │
 * ├──────┼──────────┼────────────┤    ├─────────────────┼─────────────────┤
 * │ 3001 │ Euclid   │ 2000-01-02 │ -> │ 1922-01-01      │ 2020-12-04      │
 * │ 4999 │ Keter    │ 1998-11-27 │    └─────────────────┴─────────────────┘
 * │ 5000 │ Safe     │ 2020-12-04 │
 * └──────┴──────────┴────────────┘
 *
 * finish():
 *                                     ┌─────────────────┬─────────────────┐
 *                                     │ MIN(discovered) │ MAX(discovered) │
 *                                     ├─────────────────┼─────────────────┤
 *                                     │ 1922-01-01      │ 2020-12-04      │
 *                                     └─────────────────┴─────────────────┘
 * }
 * <p>
 *     Aggregations can also filter which rows they receive using
 *     {@link FilteredAggregatorFunction}. In ESQL that looks like:
 *     {@snippet lang="esql" :
 *     | STATS emin = MIN(discovered) WHERE class == "Euclid",
 *             emax = MAX(discovered) WHERE class == "Euclid",
 *             kmin = MIN(discovered) WHERE class == "Keter"
 *     }
 *     this'd look like:
 * </p>
 * {@snippet lang="txt" :
 * Before first page:
 *                                     ┌────────────┬────────────┬────────────┐
 *                                     │ emin       │ emax       │ kmin       │
 *                                     ├────────────┼────────────┼────────────┤
 *                                     │ null       │ null       │ null       │
 *                                     └────────────┴────────────┴────────────┘
 *
 * First page:
 * ┌──────┬──────────┬────────────┐    ┌────────────┬────────────┬────────────┐
 * │  ref │ class    │ discovered │    │ emin       │ emax       │ kmin       │
 * ├──────┼──────────┼────────────┤    ├────────────┼────────────┼────────────┤
 * │  079 │ Euclid   │ 1988-01-01 │ -> │ 1988-01-01 │ 1993-01-01 │ 1967-01-01 │
 * │  173 │ Euclid   │ 1993-01-01 │    └────────────┴────────────┴────────────┘
 * │ 1313 │ Keter    │ 1967-01-01 │
 * │ 1981 │ Safe     │ 1991-01-01 │
 * └──────┴──────────┴────────────┘
 *
 * Second page:
 * ┌──────┬──────────┬────────────┐    ┌────────────┬────────────┬────────────┐
 * │  ref │ class    │ discovered │    │ emin       │ emax       │ kmin       │
 * ├──────┼──────────┼────────────┤    ├────────────┼────────────┼────────────┤
 * │ 2317 │ Keter    │ 1922-01-01 │ -> │ 1988-01-01 │ 2010-01-01 │ 1922-01-01 │
 * │ 2639 │ Euclid   │ 2010-01-01 │    └────────────┴────────────┴────────────┘
 * │ 2935 │ Keter    │ 2016-04-28 │
 * │ 3000 │ Thaumiel │ 1971-01-01 │
 * └──────┴──────────┴────────────┘
 *
 * Third page:
 * ┌──────┬──────────┬────────────┐    ┌────────────┬────────────┬────────────┐
 * │  ref │ class    │ discovered │    │ emin       │ emax       │ kmin       │
 * ├──────┼──────────┼────────────┤    ├────────────┼────────────┼────────────┤
 * │ 3001 │ Euclid   │ 2000-01-02 │ -> │ 1988-01-01 │ 2010-01-01 │ 1922-01-01 │
 * │ 4999 │ Keter    │ 1998-11-27 │    └────────────┴────────────┴────────────┘
 * │ 5000 │ Safe     │ 2020-12-04 │
 * └──────┴──────────┴────────────┘
 *
 * finish():
 *                                     ┌────────────┬────────────┬────────────┐
 *                                     │ emin       │ emax       │ kmin       │
 *                                     ├────────────┼────────────┼────────────┤
 *                                     │ 1988-01-01 │ 2010-01-01 │ 1922-01-01 │
 *                                     └────────────┴────────────┴────────────┘
 * }
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
    public boolean canProduceMoreDataWithoutExtraInput() {
        return output != null;
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
        public static final ProfileSchema<Status> PROFILE_SCHEMA = ProfileSchema.of(
            Status.class,
            (builder, status, params) -> status.writeProfile(builder)
        )
            .required("aggregation_nanos", ProfileSchema.integer())
            .optional("aggregation_time", ProfileSchema.string())
            .required("aggregation_finish_nanos", ProfileSchema.nullable(ProfileSchema.integer()))
            .optional("aggregation_finish_time", ProfileSchema.nullable(ProfileSchema.string()))
            .required("pages_processed", ProfileSchema.integer())
            .required("rows_received", ProfileSchema.integer())
            .required("rows_emitted", ProfileSchema.integer())
            .build();

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
         *
         * @param aggregationNanos       Nanoseconds this operator has spent running the aggregations.
         * @param aggregationFinishNanos Nanoseconds this operator has spent running the aggregations.
         * @param pagesProcessed         Count of pages this operator has processed.
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
            aggregationFinishNanos = in.readOptionalVLong();
            pagesProcessed = in.readVInt();
            rowsReceived = in.readVLong();
            rowsEmitted = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(aggregationNanos);
            out.writeOptionalVLong(aggregationFinishNanos);
            out.writeVInt(pagesProcessed);
            out.writeVLong(rowsReceived);
            out.writeVLong(rowsEmitted);
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

        private XContentBuilder writeProfile(XContentBuilder builder) throws IOException {
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
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return PROFILE_SCHEMA.write(builder, this, params);
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
            return TransportVersion.minimumCompatible();
        }
    }
}
