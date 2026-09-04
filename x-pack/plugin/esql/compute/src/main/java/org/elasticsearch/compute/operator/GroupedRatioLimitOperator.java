/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.PagedBytesBuilder;
import org.elasticsearch.common.bytes.PagedBytesCursor;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefHashTable;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.compute.aggregation.blockhash.HashImplFactory;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Streaming operator for {@code limit_ratio(r, v)}: retains exactly {@code ceil(r * N)} rows per
 * group of N total rows, using Bresenham-style error accumulation.
 * <p>
 * For each group, two int counters {@code (total, accepted)} are tracked. A row is accepted when
 * {@code ratio * (total + 1) > accepted}, which maintains the invariant
 * {@code accepted == ceil(ratio * total)} after every row. This is O(groups) state — no row
 * buffering.
 * <p>
 * Group keys use list semantics for multivalues: {@code [1,2]} and {@code [2,1]} are different groups.
 */
public class GroupedRatioLimitOperator implements Operator, Accountable {

    public static final class Factory implements Operator.OperatorFactory {
        private final double ratio;
        private final int[] groupChannels;
        private final List<ElementType> elementTypes;

        public Factory(double ratio, List<Integer> groupChannels, List<ElementType> elementTypes) {
            this.ratio = ratio;
            this.groupChannels = groupChannels.stream().mapToInt(Integer::intValue).toArray();
            this.elementTypes = elementTypes;
        }

        @Override
        public GroupedRatioLimitOperator get(DriverContext driverContext) {
            BlockFactory blockFactory = driverContext.blockFactory();
            PagedBytesBuilder row = new PagedBytesBuilder(
                blockFactory.bigArrays().recycler(),
                blockFactory.breaker(),
                "group-key-encoder",
                64
            );
            return new GroupedRatioLimitOperator(ratio, new GroupKeyEncoder(groupChannels, elementTypes, row), blockFactory);
        }

        @Override
        public String describe() {
            return "GroupedRatioLimitOperator[ratio=" + ratio + "]";
        }
    }

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(GroupedRatioLimitOperator.class);

    private final double ratio;
    private final GroupKeyEncoder keyEncoder;
    private BytesRefHashTable seenKeys;
    private BigArrays bigArrays;
    /** Number of rows seen so far per group ordinal. */
    private IntArray totals;
    /** Number of rows accepted so far per group ordinal. */
    private IntArray accepteds;

    private int pagesProcessed;
    private long rowsReceived;
    private long rowsEmitted;

    private Page lastOutput;
    private boolean finished;

    public GroupedRatioLimitOperator(double ratio, GroupKeyEncoder keyEncoder, BlockFactory blockFactory) {
        boolean success = false;
        try {
            this.ratio = ratio;
            this.keyEncoder = keyEncoder;
            this.bigArrays = blockFactory.bigArrays();
            this.seenKeys = HashImplFactory.newBytesRefHash(blockFactory);
            this.totals = bigArrays.newIntArray(16, false);
            this.accepteds = bigArrays.newIntArray(16, false);
            success = true;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(keyEncoder, seenKeys);
            }
        }
    }

    @Override
    public boolean needsInput() {
        return finished == false && lastOutput == null;
    }

    @Override
    public void addInput(Page page) {
        try {
            assert lastOutput == null : "has pending output page";
            int positionCount = page.getPositionCount();
            rowsReceived += positionCount;

            if (ratio <= 0.0) {
                page.releaseBlocks();
                return;
            }

            int acceptedCount = 0;
            int[] accepted = new int[positionCount];

            for (int pos = 0; pos < positionCount; pos++) {
                PagedBytesCursor key = keyEncoder.encode(page, pos);
                long hashOrd = seenKeys.add(key);
                int total;
                int acc;
                long ord;
                if (hashOrd >= 0) {
                    ord = hashOrd;
                    totals = bigArrays.grow(totals, ord + 1);
                    accepteds = bigArrays.grow(accepteds, ord + 1);
                    total = 0;
                    acc = 0;
                    totals.set(ord, 0);
                    accepteds.set(ord, 0);
                } else {
                    ord = -(hashOrd + 1);
                    total = totals.get(ord);
                    acc = accepteds.get(ord);
                }

                // Bresenham: accept if ratio * (total + 1) > accepted
                if (ratio >= 1.0 || ratio * (total + 1) > acc) {
                    totals.set(ord, total + 1);
                    accepteds.set(ord, acc + 1);
                    accepted[acceptedCount++] = pos;
                } else {
                    totals.set(ord, total + 1);
                }
            }

            if (acceptedCount == 0) {
                return;
            }

            if (acceptedCount == positionCount) {
                lastOutput = page.shallowCopy();
            } else {
                lastOutput = page.filter(false, accepted, 0, acceptedCount);
            }
        } finally {
            page.releaseBlocks();
        }
    }

    @Override
    public void finish() {
        finished = true;
    }

    @Override
    public boolean isFinished() {
        return lastOutput == null && finished;
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        return lastOutput != null;
    }

    @Override
    public Page getOutput() {
        if (lastOutput == null) {
            return null;
        }
        Page result = lastOutput;
        lastOutput = null;
        pagesProcessed++;
        rowsEmitted += result.getPositionCount();
        return result;
    }

    @Override
    public long ramBytesUsed() {
        long size = SHALLOW_SIZE;
        size += seenKeys.ramBytesUsed();
        size += totals.ramBytesUsed();
        size += accepteds.ramBytesUsed();
        size += keyEncoder.ramBytesUsed();
        return size;
    }

    @Override
    public Status status() {
        return new Status(ratio, (int) seenKeys.size(), pagesProcessed, rowsReceived, rowsEmitted, ramBytesUsed());
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(
            lastOutput == null ? () -> {} : lastOutput::releaseBlocks,
            seenKeys,
            totals,
            accepteds,
            keyEncoder
        );
    }

    @Override
    public String toString() {
        return "GroupedRatioLimitOperator[ratio="
            + ratio
            + ", groupKeys="
            + Arrays.toString(keyEncoder.groupChannels())
            + ", groups="
            + seenKeys.size()
            + "]";
    }

    public static class Status implements Operator.Status {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "grouped_ratio_limit",
            Status::new
        );

        private final double ratio;
        private final int groupCount;
        private final int pagesProcessed;
        private final long rowsReceived;
        private final long rowsEmitted;
        private final long ramBytesUsed;

        protected Status(double ratio, int groupCount, int pagesProcessed, long rowsReceived, long rowsEmitted, long ramBytesUsed) {
            this.ratio = ratio;
            this.groupCount = groupCount;
            this.pagesProcessed = pagesProcessed;
            this.rowsReceived = rowsReceived;
            this.rowsEmitted = rowsEmitted;
            this.ramBytesUsed = ramBytesUsed;
        }

        protected Status(StreamInput in) throws IOException {
            ratio = in.readDouble();
            groupCount = in.readVInt();
            pagesProcessed = in.readVInt();
            rowsReceived = in.readVLong();
            rowsEmitted = in.readVLong();
            ramBytesUsed = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeDouble(ratio);
            out.writeVInt(groupCount);
            out.writeVInt(pagesProcessed);
            out.writeVLong(rowsReceived);
            out.writeVLong(rowsEmitted);
            out.writeVLong(ramBytesUsed);
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        public double ratio() {
            return ratio;
        }

        public int groupCount() {
            return groupCount;
        }

        public int pagesProcessed() {
            return pagesProcessed;
        }

        public long rowsReceived() {
            return rowsReceived;
        }

        public long rowsEmitted() {
            return rowsEmitted;
        }

        public long ramBytesUsed() {
            return ramBytesUsed;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("ratio", ratio);
            builder.field("group_count", groupCount);
            builder.field("pages_processed", pagesProcessed);
            builder.field("rows_received", rowsReceived);
            builder.field("rows_emitted", rowsEmitted);
            builder.field("ram_bytes_used", ramBytesUsed);
            builder.field("ram_used", ByteSizeValue.ofBytes(ramBytesUsed));
            return builder.endObject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Status status = (Status) o;
            return Double.compare(ratio, status.ratio) == 0
                && groupCount == status.groupCount
                && pagesProcessed == status.pagesProcessed
                && rowsReceived == status.rowsReceived
                && rowsEmitted == status.rowsEmitted
                && ramBytesUsed == status.ramBytesUsed;
        }

        @Override
        public int hashCode() {
            return Objects.hash(ratio, groupCount, pagesProcessed, rowsReceived, rowsEmitted, ramBytesUsed);
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.minimumCompatible();
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }
}
