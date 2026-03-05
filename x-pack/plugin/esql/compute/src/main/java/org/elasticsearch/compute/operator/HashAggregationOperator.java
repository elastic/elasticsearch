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
import org.elasticsearch.compute.Describable;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.GroupingAggregatorEvaluationContext;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class HashAggregationOperator implements Operator {

    public record HashAggregationOperatorFactory(
        List<BlockHash.GroupSpec> groups,
        AggregatorMode aggregatorMode,
        List<GroupingAggregator.Factory> aggregators,
        int maxPageSize,
        int partialEmitKeysThreshold,
        double partialEmitUniquenessThreshold,
        AnalysisRegistry analysisRegistry
    ) implements OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            if (groups.stream().anyMatch(BlockHash.GroupSpec::isCategorize)) {
                return new HashAggregationOperator(
                    aggregatorMode,
                    aggregators,
                    () -> BlockHash.buildCategorizeBlockHash(
                        groups,
                        aggregatorMode,
                        driverContext.blockFactory(),
                        analysisRegistry,
                        maxPageSize
                    ),
                    Integer.MAX_VALUE, // disable the early partial emit for categorize
                    1.0,
                    driverContext
                );
            }
            return new HashAggregationOperator(
                aggregatorMode,
                aggregators,
                () -> BlockHash.build(groups, driverContext.blockFactory(), maxPageSize, false),
                partialEmitKeysThreshold,
                partialEmitUniquenessThreshold,
                driverContext
            );
        }

        @Override
        public String describe() {
            return "HashAggregationOperator[mode = "
                + "<not-needed>"
                + ", aggs = "
                + aggregators.stream().map(Describable::describe).collect(joining(", "))
                + "]";
        }
    }

    private boolean finished;
    private Page output;

    protected final Supplier<BlockHash> blockHashSupplier;
    protected final AggregatorMode aggregatorMode;
    protected final List<GroupingAggregator.Factory> aggregatorFactories;

    protected final DriverContext driverContext;

    // The blockHash and aggregators can be re-initialized when partial results are emitted periodically
    protected BlockHash blockHash;
    protected final List<GroupingAggregator> aggregators;

    /**
     * Nanoseconds this operator has spent hashing grouping keys.
     */
    private long hashNanos;
    /**
     * Nanoseconds this operator has spent running the aggregations.
     */
    private long aggregationNanos;
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

    /**
     * Total nanos for emitting the output
     */
    protected long emitNanos;

    protected long emitCount;

    protected long rowsAddedInCurrentBatch;
    protected final int partialEmitKeysThreshold;
    protected final double partialEmitUniquenessThreshold;

    @SuppressWarnings("this-escape")
    public HashAggregationOperator(
        AggregatorMode aggregatorMode,
        List<GroupingAggregator.Factory> aggregatorFactories,
        Supplier<BlockHash> blockHashSupplier,
        int partialEmitKeysThreshold,
        double partialEmitUniquenessThreshold,
        DriverContext driverContext
    ) {
        if (partialEmitKeysThreshold <= 0) {
            throw new IllegalArgumentException("partialEmitKeysThreshold must be greater than 0; got " + partialEmitKeysThreshold);
        }
        this.aggregatorMode = aggregatorMode;
        this.partialEmitKeysThreshold = partialEmitKeysThreshold;
        this.partialEmitUniquenessThreshold = partialEmitUniquenessThreshold;
        this.driverContext = driverContext;
        this.aggregatorFactories = aggregatorFactories;
        this.blockHashSupplier = blockHashSupplier;
        this.aggregators = new ArrayList<>();
        boolean success = false;
        try {
            this.blockHash = blockHashSupplier.get();
            for (GroupingAggregator.Factory a : aggregatorFactories) {
                var groupingAggregator = a.apply(driverContext);
                assert groupingAggregator.mode() == aggregatorMode : groupingAggregator.mode() + " != " + aggregatorMode;
                this.aggregators.add(groupingAggregator);
            }
            success = true;
        } finally {
            if (success == false) {
                close();
            }
        }
    }

    @Override
    public boolean needsInput() {
        return output == null && finished == false;
    }

    @Override
    public void addInput(Page page) {
        try {
            maybeReinitializeAfterPeriodicallyEmitted();
            GroupingAggregatorFunction.AddInput[] prepared = new GroupingAggregatorFunction.AddInput[aggregators.size()];
            class AddInput implements GroupingAggregatorFunction.AddInput {
                long hashStart = System.nanoTime();
                long aggStart;

                @Override
                public void add(int positionOffset, IntArrayBlock groupIds) {
                    startAggEndHash();
                    for (GroupingAggregatorFunction.AddInput p : prepared) {
                        p.add(positionOffset, groupIds);
                    }
                    end();
                }

                @Override
                public void add(int positionOffset, IntBigArrayBlock groupIds) {
                    startAggEndHash();
                    for (GroupingAggregatorFunction.AddInput p : prepared) {
                        p.add(positionOffset, groupIds);
                    }
                    end();
                }

                @Override
                public void add(int positionOffset, IntVector groupIds) {
                    startAggEndHash();
                    for (GroupingAggregatorFunction.AddInput p : prepared) {
                        p.add(positionOffset, groupIds);
                    }
                    end();
                }

                private void startAggEndHash() {
                    aggStart = System.nanoTime();
                    hashNanos += aggStart - hashStart;
                }

                private void end() {
                    hashStart = System.nanoTime();
                    aggregationNanos += hashStart - aggStart;
                }

                @Override
                public void close() {
                    Releasables.closeExpectNoException(prepared);
                }
            }
            try (AddInput add = new AddInput()) {
                checkState(needsInput(), "Operator is already finishing");
                requireNonNull(page, "page is null");

                for (int i = 0; i < prepared.length; i++) {
                    prepared[i] = aggregators.get(i).prepareProcessPage(blockHash, page);
                }

                blockHash.add(wrapPage(page), add);
                hashNanos += System.nanoTime() - add.hashStart;
            }
            rowsAddedInCurrentBatch += page.getPositionCount();
            if (shouldEmitPartialResultsPeriodically()) {
                emit();
            }
        } finally {
            page.releaseBlocks();
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
        output = null;
        return p;
    }

    @Override
    public void finish() {
        if (finished) {
            return;
        }
        finished = true;
        emit();
    }

    private void maybeReinitializeAfterPeriodicallyEmitted() {
        if (rowsReceived > 0 && rowsAddedInCurrentBatch == 0) {
            blockHash.close();
            blockHash = null;
            blockHash = blockHashSupplier.get();
            for (int i = 0; i < aggregators.size(); i++) {
                Releasables.close(aggregators.set(i, aggregatorFactories.get(i).apply(driverContext)));
            }
        }
    }

    protected void emit() {
        if (rowsAddedInCurrentBatch == 0) {
            return;
        }
        Block[] blocks = null;
        IntVector selected = null;
        long startInNanos = System.nanoTime();
        boolean success = false;
        try {
            selected = blockHash.nonEmpty();
            Block[] keys = blockHash.getKeys();
            int[] aggBlockCounts = aggregators.stream().mapToInt(GroupingAggregator::evaluateBlockCount).toArray();
            blocks = new Block[keys.length + Arrays.stream(aggBlockCounts).sum()];
            System.arraycopy(keys, 0, blocks, 0, keys.length);
            int offset = keys.length;
            try (var evaluationContext = evaluationContext(blockHash, keys)) {
                for (int i = 0; i < aggregators.size(); i++) {
                    var aggregator = aggregators.get(i);
                    evaluateAggregator(aggregator, blocks, offset, selected, evaluationContext);
                    offset += aggBlockCounts[i];
                }
                output = new Page(blocks);
                success = true;
            }
        } finally {
            rowsAddedInCurrentBatch = 0;
            // selected should always be closed
            Releasables.close(selected);
            if (success == false && blocks != null) {
                Releasables.closeExpectNoException(blocks);
            }
            emitNanos += System.nanoTime() - startInNanos;
            emitCount++;
        }
    }

    protected boolean shouldEmitPartialResultsPeriodically() {
        if (aggregatorMode.isOutputPartial() == false) {
            return false;
        }
        if (rowsAddedInCurrentBatch == 0) {
            return false;
        }
        final int numKeys = blockHash.numKeys();
        if (numKeys < partialEmitKeysThreshold) {
            return false;
        }
        return rowsAddedInCurrentBatch * partialEmitUniquenessThreshold <= numKeys;
    }

    protected void evaluateAggregator(
        GroupingAggregator aggregator,
        Block[] blocks,
        int offset,
        IntVector selected,
        GroupingAggregatorEvaluationContext evaluationContext
    ) {
        aggregator.evaluate(blocks, offset, selected, evaluationContext);
    }

    protected GroupingAggregatorEvaluationContext evaluationContext(BlockHash blockHash, Block[] keys) {
        return new GroupingAggregatorEvaluationContext(driverContext);
    }

    @Override
    public boolean isFinished() {
        return finished && output == null;
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        return output != null;
    }

    @Override
    public void close() {
        if (output != null) {
            output.releaseBlocks();
        }
        Releasables.close(blockHash, () -> Releasables.close(aggregators));
    }

    @Override
    public Operator.Status status() {
        return new Status(hashNanos, aggregationNanos, pagesProcessed, rowsReceived, rowsEmitted, emitNanos, emitCount);
    }

    protected static void checkState(boolean condition, String msg) {
        if (condition == false) {
            throw new IllegalArgumentException(msg);
        }
    }

    protected Page wrapPage(Page page) {
        return page;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass().getSimpleName()).append("[");
        sb.append("blockHash=").append(blockHash).append(", ");
        sb.append("aggregators=").append(aggregators);
        sb.append("]");
        return sb.toString();
    }

    public static class Status implements Operator.Status {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "hashagg",
            Status::new
        );

        private static final TransportVersion ESQL_HASH_OPERATOR_STATUS_OUTPUT_TIME = TransportVersion.fromName(
            "esql_hash_operator_status_output_time"
        );

        private static final TransportVersion ESQL_HASH_OPERATOR_STATUS_EMIT_COUNT = TransportVersion.fromName(
            "esql_hash_operator_status_emit_count"
        );

        /**
         * Nanoseconds this operator has spent hashing grouping keys.
         */
        private final long hashNanos;
        /**
         * Nanoseconds this operator has spent running the aggregations.
         */
        private final long aggregationNanos;
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

        private final long emitNanos;

        protected final long emitCount;

        /**
         * Build.
         *
         * @param hashNanos        Nanoseconds this operator has spent hashing grouping keys.
         * @param aggregationNanos Nanoseconds this operator has spent running the aggregations.
         * @param pagesProcessed   Count of pages this operator has processed.
         * @param rowsReceived     Count of rows this operator has received.
         * @param rowsEmitted      Count of rows this operator has emitted.
         * @param emitNanos        Nanoseconds this operator has spent emitting the output.
         * @param emitCount        Count of times this operator has emitted output.
         */
        public Status(
            long hashNanos,
            long aggregationNanos,
            int pagesProcessed,
            long rowsReceived,
            long rowsEmitted,
            long emitNanos,
            long emitCount
        ) {
            this.hashNanos = hashNanos;
            this.aggregationNanos = aggregationNanos;
            this.pagesProcessed = pagesProcessed;
            this.rowsReceived = rowsReceived;
            this.rowsEmitted = rowsEmitted;
            this.emitNanos = emitNanos;
            this.emitCount = emitCount;
        }

        protected Status(StreamInput in) throws IOException {
            hashNanos = in.readVLong();
            aggregationNanos = in.readVLong();
            pagesProcessed = in.readVInt();
            rowsReceived = in.readVLong();
            rowsEmitted = in.readVLong();
            if (in.getTransportVersion().supports(ESQL_HASH_OPERATOR_STATUS_OUTPUT_TIME)) {
                emitNanos = in.readVLong();
            } else {
                emitNanos = 0;
            }
            if (in.getTransportVersion().supports(ESQL_HASH_OPERATOR_STATUS_EMIT_COUNT)) {
                emitCount = in.readVLong();
            } else {
                emitCount = 0;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(hashNanos);
            out.writeVLong(aggregationNanos);
            out.writeVInt(pagesProcessed);
            out.writeVLong(rowsReceived);
            out.writeVLong(rowsEmitted);
            if (out.getTransportVersion().supports(ESQL_HASH_OPERATOR_STATUS_OUTPUT_TIME)) {
                out.writeVLong(emitNanos);
            }
            if (out.getTransportVersion().supports(ESQL_HASH_OPERATOR_STATUS_EMIT_COUNT)) {
                out.writeVLong(emitCount);
            }
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        /**
         * Nanoseconds this operator has spent hashing grouping keys.
         */
        public long hashNanos() {
            return hashNanos;
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

        /**
         * Nanoseconds this operator has spent emitting the output.
         */
        public long emitNanos() {
            return emitNanos;
        }

        /**
         * Count of times this operator has emitted output.
         */
        public long emitCount() {
            return emitCount;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("hash_nanos", hashNanos);
            if (builder.humanReadable()) {
                builder.field("hash_time", TimeValue.timeValueNanos(hashNanos));
            }
            builder.field("aggregation_nanos", aggregationNanos);
            if (builder.humanReadable()) {
                builder.field("aggregation_time", TimeValue.timeValueNanos(aggregationNanos));
            }
            builder.field("pages_processed", pagesProcessed);
            builder.field("rows_received", rowsReceived);
            builder.field("rows_emitted", rowsEmitted);
            if (emitCount > 0) {
                builder.field("emit_count", emitCount);
            }
            builder.field("emit_nanos", emitNanos);
            if (builder.humanReadable()) {
                builder.field("emit_time", TimeValue.timeValueNanos(emitNanos));
            }
            return builder.endObject();

        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Status status = (Status) o;
            return hashNanos == status.hashNanos
                && aggregationNanos == status.aggregationNanos
                && pagesProcessed == status.pagesProcessed
                && rowsReceived == status.rowsReceived
                && rowsEmitted == status.rowsEmitted
                && emitNanos == status.emitNanos
                && emitCount == status.emitCount;
        }

        @Override
        public int hashCode() {
            return Objects.hash(hashNanos, aggregationNanos, pagesProcessed, rowsReceived, rowsEmitted, emitNanos, emitCount);
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
