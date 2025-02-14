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
import org.elasticsearch.compute.Describable;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;
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
        AnalysisRegistry analysisRegistry
    ) implements OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            if (groups.stream().anyMatch(BlockHash.GroupSpec::isCategorize)) {
                return new HashAggregationOperator(
                    aggregators,
                    () -> BlockHash.buildCategorizeBlockHash(
                        groups,
                        aggregatorMode,
                        driverContext.blockFactory(),
                        analysisRegistry,
                        maxPageSize
                    ),
                    Integer.MAX_VALUE, // TODO: doesn't support chunk yet
                    driverContext
                );
            }
            return new HashAggregationOperator(
                aggregators,
                () -> BlockHash.build(groups, driverContext.blockFactory(), maxPageSize, false),
                maxPageSize,
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

    private final int maxPageSize;
    private Emitter emitter;

    private boolean blockHashClosed = false;
    private final BlockHash blockHash;

    private final List<GroupingAggregator> aggregators;

    private final DriverContext driverContext;

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

    @SuppressWarnings("this-escape")
    public HashAggregationOperator(
        List<GroupingAggregator.Factory> aggregators,
        Supplier<BlockHash> blockHash,
        int maxPageSize,
        DriverContext driverContext
    ) {
        this.maxPageSize = maxPageSize;
        this.aggregators = new ArrayList<>(aggregators.size());
        this.driverContext = driverContext;
        boolean success = false;
        try {
            this.blockHash = blockHash.get();
            for (GroupingAggregator.Factory a : aggregators) {
                this.aggregators.add(a.apply(driverContext));
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
        return emitter == null;
    }

    @Override
    public void addInput(Page page) {
        try {
            GroupingAggregatorFunction.AddInput[] prepared = new GroupingAggregatorFunction.AddInput[aggregators.size()];
            class AddInput implements GroupingAggregatorFunction.AddInput {
                long hashStart = System.nanoTime();
                long aggStart;

                @Override
                public void add(int positionOffset, IntBlock groupIds) {
                    IntVector groupIdsVector = groupIds.asVector();
                    if (groupIdsVector != null) {
                        add(positionOffset, groupIdsVector);
                    } else {
                        startAggEndHash();
                        for (GroupingAggregatorFunction.AddInput p : prepared) {
                            p.add(positionOffset, groupIds);
                        }
                        end();
                    }
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
        } finally {
            page.releaseBlocks();
            pagesProcessed++;
            rowsReceived += page.getPositionCount();
        }
    }

    @Override
    public Page getOutput() {
        if (emitter == null) {
            return null;
        }
        return emitter.nextPage();
    }

    private class Emitter implements Releasable {
        private final int[] aggBlockCounts;
        private int position = -1;
        private IntVector allSelected = null;
        private Block[] allKeys;

        Emitter(int[] aggBlockCounts) {
            this.aggBlockCounts = aggBlockCounts;
        }

        Page nextPage() {
            if (position == -1) {
                position = 0;
                // TODO: chunk selected and keys
                allKeys = blockHash.getKeys();
                allSelected = blockHash.nonEmpty();
                blockHashClosed = true;
                blockHash.close();
            }
            final int endPosition = Math.toIntExact(Math.min(position + (long) maxPageSize, allSelected.getPositionCount()));
            if (endPosition == position) {
                return null;
            }
            final boolean singlePage = position == 0 && endPosition == allSelected.getPositionCount();
            final Block[] blocks = new Block[allKeys.length + Arrays.stream(aggBlockCounts).sum()];
            IntVector selected = null;
            boolean success = false;
            try {
                if (singlePage) {
                    this.allSelected.incRef();
                    selected = this.allSelected;
                    for (int i = 0; i < allKeys.length; i++) {
                        allKeys[i].incRef();
                        blocks[i] = allKeys[i];
                    }
                } else {
                    final int[] positions = new int[endPosition - position];
                    for (int i = 0; i < positions.length; i++) {
                        positions[i] = position + i;
                    }
                    // TODO: allow to filter with IntVector
                    selected = allSelected.filter(positions);
                    for (int keyIndex = 0; keyIndex < allKeys.length; keyIndex++) {
                        blocks[keyIndex] = allKeys[keyIndex].filter(positions);
                    }
                }
                int blockOffset = allKeys.length;
                for (int i = 0; i < aggregators.size(); i++) {
                    aggregators.get(i).evaluate(blocks, blockOffset, selected, driverContext);
                    blockOffset += aggBlockCounts[i];
                }
                var output = new Page(blocks);
                rowsEmitted += output.getPositionCount();
                success = true;
                return output;
            } finally {
                position = endPosition;
                Releasables.close(selected, success ? null : Releasables.wrap(blocks));
            }
        }

        @Override
        public void close() {
            Releasables.close(allSelected, allKeys != null ? Releasables.wrap(allKeys) : null);
        }

        boolean doneEmitting() {
            return allSelected != null && position >= allSelected.getPositionCount();
        }
    }

    @Override
    public void finish() {
        if (emitter == null) {
            emitter = new Emitter(aggregators.stream().mapToInt(GroupingAggregator::evaluateBlockCount).toArray());
        }
    }

    @Override
    public boolean isFinished() {
        return emitter != null && emitter.doneEmitting();
    }

    @Override
    public void close() {
        Releasables.close(emitter, blockHashClosed ? null : blockHash, () -> Releasables.close(aggregators));
    }

    @Override
    public Operator.Status status() {
        return new Status(hashNanos, aggregationNanos, pagesProcessed, rowsReceived, rowsEmitted);
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

        /**
         * Build.
         * @param hashNanos Nanoseconds this operator has spent hashing grouping keys.
         * @param aggregationNanos Nanoseconds this operator has spent running the aggregations.
         * @param pagesProcessed Count of pages this operator has processed.
         * @param rowsReceived Count of rows this operator has received.
         * @param rowsEmitted Count of rows this operator has emitted.
         */
        public Status(long hashNanos, long aggregationNanos, int pagesProcessed, long rowsReceived, long rowsEmitted) {
            this.hashNanos = hashNanos;
            this.aggregationNanos = aggregationNanos;
            this.pagesProcessed = pagesProcessed;
            this.rowsReceived = rowsReceived;
            this.rowsEmitted = rowsEmitted;
        }

        protected Status(StreamInput in) throws IOException {
            hashNanos = in.readVLong();
            aggregationNanos = in.readVLong();
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
            out.writeVLong(hashNanos);
            out.writeVLong(aggregationNanos);
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
                && rowsEmitted == status.rowsEmitted;
        }

        @Override
        public int hashCode() {
            return Objects.hash(hashNanos, aggregationNanos, pagesProcessed, rowsReceived, rowsEmitted);
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
