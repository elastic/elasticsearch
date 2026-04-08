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
 * An operator that limits the number of rows emitted per group.
 * For {@code LIMIT N BY x}, this accepts up to N rows for each distinct value of x.
 * Group keys use list semantics for multivalues: {@code [1,2]} and {@code [2,1]} are different groups.
 */
public class GroupedLimitOperator implements Operator, Accountable {
    public static final class Factory implements Operator.OperatorFactory {
        private final int limitPerGroup;
        private final int[] groupChannels;
        private final List<ElementType> elementTypes;

        public Factory(int limitPerGroup, List<Integer> groupChannels, List<ElementType> elementTypes) {
            this.limitPerGroup = limitPerGroup;
            this.groupChannels = groupChannels.stream().mapToInt(Integer::intValue).toArray();
            this.elementTypes = elementTypes;
        }

        @Override
        public GroupedLimitOperator get(DriverContext driverContext) {
            BlockFactory blockFactory = driverContext.blockFactory();
            var keyEncoder = new GroupKeyEncoder(groupChannels, elementTypes, blockFactory.breaker(), blockFactory.bigArrays().recycler());
            return new GroupedLimitOperator(limitPerGroup, keyEncoder, blockFactory);
        }

        @Override
        public String describe() {
            return "GroupedLimitOperator[limitPerGroup = " + limitPerGroup + "]";
        }
    }

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(GroupedLimitOperator.class);

    private final int limitPerGroup;
    private final GroupKeyEncoder keyEncoder;
    /**
     * BlockHash unrolls multivalues (i.e. for each unique element of a multivalue [1,2,2,1] it would create
     * a new key).
     *
     * <p>In contrast, in this operator we consider every multivalue as a key. Two multivalues are equal if they hold
     * the same values in the exact same order. That's why we need to use:
     * <ul>
     *   <li>a ref hash table. Every time we add a new key it gives a monotonically increasing integer that we will
     *       use to index in the counts array, {@code Ordinal(key)}</li>
     *   <li>a counts array to keep track of the number of times we have seen each {@code Ordinal(key)}</li>
     * </ul>
     */
    private BytesRefHashTable seenKeys;
    private BigArrays bigArrays;
    private IntArray counts;

    private int pagesProcessed;
    private long rowsReceived;
    private long rowsEmitted;

    private Page lastOutput;
    private boolean finished;

    public GroupedLimitOperator(int limitPerGroup, GroupKeyEncoder keyEncoder, BlockFactory blockFactory) {
        boolean success = false;
        try {
            this.limitPerGroup = limitPerGroup;
            this.keyEncoder = keyEncoder;
            this.bigArrays = blockFactory.bigArrays();
            this.seenKeys = HashImplFactory.newBytesRefHash(blockFactory);
            this.counts = bigArrays.newIntArray(16, false);
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

            if (limitPerGroup == 0) {
                page.releaseBlocks();
                return;
            }

            int acceptedCount = 0;
            int[] accepted = new int[positionCount];

            for (int pos = 0; pos < positionCount; pos++) {
                long hashOrd = keyEncoder.encodeAndAdd(page, pos, seenKeys);
                int count;
                long ord;
                if (hashOrd >= 0) {
                    ord = hashOrd;
                    counts = bigArrays.grow(counts, ord + 1);
                    count = 0;
                    counts.set(ord, 0);
                } else {
                    ord = -(hashOrd + 1);
                    count = counts.get(ord);
                }
                if (count < limitPerGroup) {
                    counts.set(ord, count + 1);
                    accepted[acceptedCount++] = pos;
                }
            }

            if (acceptedCount == 0) {
                return;
            }

            /*
             * When all rows in a page are accepted the operator returns the
             * original page instance rather than a filtered copy.
             */
            if (acceptedCount == positionCount) {
                lastOutput = page.shallowCopy();
            } else {
                int[] positions = new int[acceptedCount];
                System.arraycopy(accepted, 0, positions, 0, acceptedCount);
                lastOutput = page.filter(false, positions);
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
        size += counts.ramBytesUsed();
        size += keyEncoder.ramBytesUsed();
        return size;
    }

    @Override
    public Status status() {
        return new Status(limitPerGroup, (int) seenKeys.size(), pagesProcessed, rowsReceived, rowsEmitted, ramBytesUsed());
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(lastOutput == null ? () -> {} : lastOutput::releaseBlocks, seenKeys, counts, keyEncoder);
    }

    @Override
    public String toString() {
        return "GroupedLimitOperator[limitPerGroup = "
            + limitPerGroup
            + ", groupKeys = "
            + Arrays.toString(keyEncoder.groupChannels())
            + ", groups = "
            + seenKeys.size()
            + "]";
    }

    public static class Status implements Operator.Status {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "grouped_limit",
            Status::new
        );

        private final int limitPerGroup;
        private final int groupCount;
        private final int pagesProcessed;
        private final long rowsReceived;
        private final long rowsEmitted;
        private final long ramBytesUsed;

        protected Status(int limitPerGroup, int groupCount, int pagesProcessed, long rowsReceived, long rowsEmitted, long ramBytesUsed) {
            this.limitPerGroup = limitPerGroup;
            this.groupCount = groupCount;
            this.pagesProcessed = pagesProcessed;
            this.rowsReceived = rowsReceived;
            this.rowsEmitted = rowsEmitted;
            this.ramBytesUsed = ramBytesUsed;
        }

        protected Status(StreamInput in) throws IOException {
            limitPerGroup = in.readVInt();
            groupCount = in.readVInt();
            pagesProcessed = in.readVInt();
            rowsReceived = in.readVLong();
            rowsEmitted = in.readVLong();
            ramBytesUsed = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(limitPerGroup);
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

        public int limitPerGroup() {
            return limitPerGroup;
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
            builder.field("limit_per_group", limitPerGroup);
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
            return limitPerGroup == status.limitPerGroup
                && groupCount == status.groupCount
                && pagesProcessed == status.pagesProcessed
                && rowsReceived == status.rowsReceived
                && rowsEmitted == status.rowsEmitted
                && ramBytesUsed == status.ramBytesUsed;
        }

        @Override
        public int hashCode() {
            return Objects.hash(limitPerGroup, groupCount, pagesProcessed, rowsReceived, rowsEmitted, ramBytesUsed);
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
