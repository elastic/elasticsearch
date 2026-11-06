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
import org.elasticsearch.common.breaker.CircuitBreakingException;
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
import org.elasticsearch.compute.spill.QueryBreaker;
import org.elasticsearch.compute.spill.QueryScope;
import org.elasticsearch.compute.spill.SpillStatus;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * An operator that limits the number of rows emitted per group.
 * For {@code LIMIT N BY x}, this accepts up to N rows for each distinct value of x.
 * Group keys use list semantics for multivalues: {@code [1,2]} and {@code [2,1]} are different groups.
 * <p>
 * Spilling, when the driver's query has a scope that spills: the operator acquires its headroom, a run writer with its
 * buffers, once its own state reaches the query's own-state minimum, and it freezes its key set when the query would
 * cross its budget, or when the reservation it takes before a page fails. From the freeze on, a row of a key seen
 * before keeps streaming in arrival order, counted as before, and a row of any other key goes to one of
 * {@link SpillReplay#PARTITIONS} runs chosen by a hash of the key bytes already in hand, so memory stops growing at that moment.
 * After the input ends the frozen set is released and each partition is replayed, in arrival order, through a fresh
 * operator with the whole budget, which may freeze and partition again with another seed, up to {@link SpillReplay#MAX_DEPTH}.
 * Nothing of the key set is ever serialized: the keys in the runs are disjoint from the frozen keys by construction, so
 * no emitted row is emitted twice and no count travels to disk. Output order changes for the keys first seen after
 * the freeze only, which {@code LIMIT BY} and {@code DEDUP} allow without {@code SORT}.
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
            PagedBytesBuilder row = new PagedBytesBuilder(
                blockFactory.bigArrays().recycler(),
                blockFactory.breaker(),
                "group-key-encoder",
                64
            );
            return new GroupedLimitOperator(
                limitPerGroup,
                new GroupKeyEncoder(groupChannels, elementTypes, row),
                blockFactory,
                driverContext,
                0,
                0
            );
        }

        @Override
        public String describe() {
            return "GroupedLimitOperator[limitPerGroup = " + limitPerGroup + "]";
        }
    }

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(GroupedLimitOperator.class);

    private final int limitPerGroup;
    private final GroupKeyEncoder keyEncoder;
    private final BlockFactory blockFactory;
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
     * Both are released at the end of the input once frozen, since the replay needs neither.
     */
    @Nullable
    private BytesRefHashTable seenKeys;
    private BigArrays bigArrays;
    @Nullable
    private IntArray counts;
    private long frozenGroupCount;

    private int pagesProcessed;
    private long rowsReceived;
    private long rowsEmitted;

    private Page lastOutput;
    private boolean finished;

    // ---- spilling; every field below is touched by the driver thread only
    @Nullable
    private final DriverContext driverContext;
    /** The query's scope when it spills, null otherwise: the pre-feature paths then run untouched. */
    @Nullable
    private final QueryScope scope;
    /** The overflow and the replay, present exactly when {@link #scope} is. */
    @Nullable
    private final SpillReplay spill;
    /** A page admitted after the freeze while the writer was still being created; routed when it exists. */
    @Nullable
    private Page pending;

    /**
     * An operator outside any query scope: today's paths, nothing spills.
     */
    public GroupedLimitOperator(int limitPerGroup, GroupKeyEncoder keyEncoder, BlockFactory blockFactory) {
        this(limitPerGroup, keyEncoder, blockFactory, null, 0, 0);
    }

    /**
     * @param driverContext the driver's context, whose query scope decides whether this operator may spill
     * @param depth how many freezes this operator's input went through already; zero for the planned operator
     * @param seed changes the partition routing at each depth, so a partition that freezes again splits differently
     */
    @SuppressWarnings("this-escape")
    GroupedLimitOperator(
        int limitPerGroup,
        GroupKeyEncoder keyEncoder,
        BlockFactory blockFactory,
        @Nullable DriverContext driverContext,
        int depth,
        int seed
    ) {
        boolean success = false;
        try {
            this.limitPerGroup = limitPerGroup;
            this.keyEncoder = keyEncoder;
            this.blockFactory = blockFactory;
            this.bigArrays = blockFactory.bigArrays();
            this.seenKeys = HashImplFactory.newBytesRefHash(blockFactory);
            this.counts = bigArrays.newIntArray(16, false);
            this.driverContext = driverContext;
            QueryScope queryScope = driverContext == null ? null : driverContext.queryScope();
            this.scope = SpillReplay.applies(queryScope) ? queryScope : null;
            // the replay's host is this operator; it reads nothing of it before the constructor is done
            this.spill = this.scope == null
                ? null
                : new SpillReplay(driverContext, this.scope, new ReplayHost(), "grouped-limit", depth, seed);
            success = true;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(keyEncoder, seenKeys);
            }
        }
    }

    @Override
    public boolean needsInput() {
        return finished == false && lastOutput == null && pending == null && (spill == null || spill.appendsBlocked().listener().isDone());
    }

    @Override
    public void addInput(Page page) {
        if (scope == null) {
            addInputResident(page);
            return;
        }
        // the page is the operator's from here on: a throw before it was admitted, routed or held releases it
        boolean handedOver = false;
        try {
            spill.checkSpillFailure();
            assert lastOutput == null : "has pending output page";
            assert pending == null : "has a page held";
            rowsReceived += page.getPositionCount();
            if (limitPerGroup == 0) {
                handedOver = true;
                page.releaseBlocks();
                return;
            }
            if (spill.frozen()) {
                handedOver = true;
                routeOrHold(page);
                return;
            }
            spill.maybeAcquireHeadroom();
            spill.resolveHeadroom();
            long estimate = estimateFor(page);
            if (spill.shouldFreeze(estimate)) {
                freeze("the query would cross its budget");
                handedOver = true;
                routeOrHold(page);
                return;
            }
            // the reservation before the page is the backstop at a safe point: it fails here, with the table and the
            // counts untouched, rather than in the middle of the page; with headroom in hand that failure is the
            // freeze, without it the query fails as it does today
            QueryBreaker.View view = driverContext.queryView();
            boolean reserved = false;
            if (view != null) {
                try {
                    view.reserve(estimate, "grouped limit page");
                    reserved = true;
                } catch (CircuitBreakingException e) {
                    if (spill.mayFreeze()) {
                        freeze("the reservation before the page failed");
                        handedOver = true;
                        routeOrHold(page);
                        return;
                    }
                    throw e;
                }
            }
            try {
                handedOver = true;
                admit(page);
            } finally {
                if (reserved) {
                    view.releaseReserve();
                }
            }
        } catch (RuntimeException e) {
            if (handedOver == false) {
                page.releaseBlocks();
            }
            throw e;
        }
    }

    /**
     * The pre-feature path, untouched: for an operator outside any query scope.
     */
    private void addInputResident(Page page) {
        assert lastOutput == null : "has pending output page";
        rowsReceived += page.getPositionCount();
        if (limitPerGroup == 0) {
            page.releaseBlocks();
            return;
        }
        admit(page);
    }

    /**
     * Today's loop: every key is added to the table and counted, and the accepted positions become the output.
     */
    private void admit(Page page) {
        try {
            int positionCount = page.getPositionCount();
            int acceptedCount = 0;
            int[] accepted = new int[positionCount];

            for (int pos = 0; pos < positionCount; pos++) {
                PagedBytesCursor key = keyEncoder.encode(page, pos);
                long hashOrd = seenKeys.add(key);
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
                lastOutput = page.filter(false, accepted, 0, acceptedCount);
            }
        } finally {
            page.releaseBlocks();
        }
    }

    // ---- the freeze

    /**
     * An upper bound of what the page may add to the operator's state: a doubling of the table and the counts, the key
     * bytes of the page, and the filtered output page, which is at most a copy of the input.
     */
    private long estimateFor(Page page) {
        long keyBytes = 0;
        for (int channel : keyEncoder.groupChannels()) {
            keyBytes += page.getBlock(channel).ramBytesUsed();
        }
        return seenKeys.ramBytesUsed() + counts.ramBytesUsed() + keyBytes + page.ramBytesUsedByBlocks();
    }

    private void freeze(String reason) {
        frozenGroupCount = seenKeys.size();
        spill.freeze(reason, frozenGroupCount);
    }

    private void routeOrHold(Page page) {
        boolean ready;
        try {
            ready = spill.writerReady();
        } catch (RuntimeException e) {
            page.releaseBlocks();
            throw e;
        }
        if (ready) {
            route(page);
        } else {
            pending = page;
        }
    }

    /**
     * The frozen loop: a present key keeps its count and streams, an absent key's row goes to its partition's run.
     */
    private void route(Page page) {
        try {
            int positionCount = page.getPositionCount();
            int acceptedCount = 0;
            int[] accepted = new int[positionCount];
            int[][] byPartition = new int[SpillReplay.PARTITIONS][];
            int[] partitionCounts = new int[SpillReplay.PARTITIONS];
            for (int pos = 0; pos < positionCount; pos++) {
                PagedBytesCursor key = keyEncoder.encode(page, pos);
                long ord = seenKeys.find(key);
                if (ord >= 0) {
                    int count = counts.get(ord);
                    if (count < limitPerGroup) {
                        counts.set(ord, count + 1);
                        accepted[acceptedCount++] = pos;
                    }
                } else {
                    int partition = spill.partitionOf(key);
                    int[] positions = byPartition[partition];
                    if (positions == null) {
                        positions = new int[8];
                        byPartition[partition] = positions;
                    } else if (partitionCounts[partition] == positions.length) {
                        positions = Arrays.copyOf(positions, positions.length * 2);
                        byPartition[partition] = positions;
                    }
                    positions[partitionCounts[partition]++] = pos;
                }
            }
            if (acceptedCount == positionCount) {
                lastOutput = page.shallowCopy();
                return;
            }
            if (acceptedCount > 0) {
                lastOutput = page.filter(false, accepted, 0, acceptedCount);
            }
            for (int partition = 0; partition < SpillReplay.PARTITIONS; partition++) {
                if (partitionCounts[partition] == 0) {
                    continue;
                }
                Page part = page.filter(false, byPartition[partition], 0, partitionCounts[partition]);
                try {
                    spill.append(partition, part);
                } finally {
                    part.releaseBlocks();
                }
            }
        } finally {
            page.releaseBlocks();
        }
    }

    // ---- the end of the input and the replay

    @Override
    public void finish() {
        finished = true;
        if (spill != null) {
            spill.maybeStartFinishing(true, pending != null);
        }
    }

    @Override
    public boolean isFinished() {
        if (spill == null || spill.frozen() == false) {
            return lastOutput == null && finished;
        }
        return finished && lastOutput == null && pending == null && spill.replayDone();
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        return lastOutput != null || pending != null || (spill != null && spill.frozen() && finished);
    }

    @Override
    public IsBlockedResult isBlocked() {
        if (spill == null || lastOutput != null) {
            return NOT_BLOCKED;
        }
        return spill.blocked(pending != null);
    }

    @Override
    public Page getOutput() {
        if (spill == null) {
            return takeLastOutput();
        }
        spill.checkSpillFailure();
        if (lastOutput != null) {
            return takeLastOutput();
        }
        if (pending != null) {
            if (spill.writerReady() == false) {
                return null;
            }
            Page page = pending;
            pending = null;
            route(page);
            spill.maybeStartFinishing(finished, false);
            if (lastOutput != null) {
                return takeLastOutput();
            }
        }
        if (spill.frozen() == false || finished == false) {
            return null;
        }
        Page out = spill.advanceReplay();
        if (out != null) {
            pagesProcessed++;
            rowsEmitted += out.getPositionCount();
        }
        return out;
    }

    private Page takeLastOutput() {
        if (lastOutput == null) {
            return null;
        }
        Page result = lastOutput;
        lastOutput = null;
        pagesProcessed++;
        rowsEmitted += result.getPositionCount();
        return result;
    }

    private void releaseFrozenState() {
        Releasables.close(seenKeys, counts);
        seenKeys = null;
        counts = null;
    }

    /**
     * What the replay needs of this operator: its size, a child for a partition, the frozen state's release and a
     * child's counters.
     */
    private final class ReplayHost implements SpillReplay.Host {
        @Override
        public long ownBytes() {
            return ramBytesUsed();
        }

        @Override
        public Operator newReplay(int depth, int seed) {
            return new GroupedLimitOperator(limitPerGroup, keyEncoder.copy(blockFactory), blockFactory, driverContext, depth, seed);
        }

        @Override
        public void releaseFrozenState() {
            GroupedLimitOperator.this.releaseFrozenState();
        }

        @Override
        public SpillStatus spillStatusOf(Operator replay) {
            return ((GroupedLimitOperator) replay).spillStatus();
        }
    }

    // ---- accounting and status

    @Override
    public long ramBytesUsed() {
        long size = SHALLOW_SIZE;
        if (seenKeys != null) {
            size += seenKeys.ramBytesUsed();
        }
        if (counts != null) {
            size += counts.ramBytesUsed();
        }
        size += keyEncoder.ramBytesUsed();
        if (spill != null) {
            size += spill.replayBytes();
        }
        return size;
    }

    private long groupCount() {
        return seenKeys != null ? seenKeys.size() : frozenGroupCount;
    }

    /**
     * This operator's spill counters with those of its replay operators, gone and current.
     */
    SpillStatus spillStatus() {
        return spill == null ? SpillStatus.NONE : spill.spillStatus();
    }

    @Override
    public Status status() {
        return new Status(
            limitPerGroup,
            (int) groupCount(),
            pagesProcessed,
            rowsReceived,
            rowsEmitted,
            ramBytesUsed(),
            spill == null ? 0 : spill.rowsSpilled(),
            spill == null ? 0 : spill.partitionsReplayed(),
            spillStatus()
        );
    }

    @Override
    public void close() {
        Releasable lastOutputRelease = lastOutput == null ? () -> {} : lastOutput::releaseBlocks;
        Releasable pendingRelease = pending == null ? () -> {} : pending::releaseBlocks;
        Releasables.closeExpectNoException(lastOutputRelease, pendingRelease, spill, seenKeys, counts, keyEncoder);
        lastOutput = null;
        pending = null;
        seenKeys = null;
        counts = null;
    }

    @Override
    public String toString() {
        return "GroupedLimitOperator[limitPerGroup = "
            + limitPerGroup
            + ", groupKeys = "
            + Arrays.toString(keyEncoder.groupChannels())
            + ", groups = "
            + groupCount()
            + "]";
    }

    public static class Status implements Operator.Status {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "grouped_limit",
            Status::new
        );
        public static final ProfileSchema<Status> PROFILE_SCHEMA = SpillStatus.addTo(
            ProfileSchema.of(Status.class, (builder, status, params) -> status.writeProfile(builder))
                .required("limit_per_group", ProfileSchema.integer())
                .required("group_count", ProfileSchema.integer())
                .required("pages_processed", ProfileSchema.integer())
                .required("rows_received", ProfileSchema.integer())
                .required("rows_emitted", ProfileSchema.integer())
                .required("ram_bytes_used", ProfileSchema.integer())
                .required("ram_used", ProfileSchema.string())
                .optional("rows_spilled", ProfileSchema.integer())
                .optional("partitions_replayed", ProfileSchema.integer())
        ).build();

        private final int limitPerGroup;
        private final int groupCount;
        private final int pagesProcessed;
        private final long rowsReceived;
        private final long rowsEmitted;
        private final long ramBytesUsed;
        private final long rowsSpilled;
        private final int partitionsReplayed;
        private final SpillStatus spill;

        protected Status(int limitPerGroup, int groupCount, int pagesProcessed, long rowsReceived, long rowsEmitted, long ramBytesUsed) {
            this(limitPerGroup, groupCount, pagesProcessed, rowsReceived, rowsEmitted, ramBytesUsed, 0, 0, SpillStatus.NONE);
        }

        /**
         * @param rowsSpilled rows routed to a partition run after the freeze, at every depth
         * @param partitionsReplayed partitions replayed through a child operator, at every depth
         * @param spill the shared spill counters, this operator's and its replay operators' together
         */
        protected Status(
            int limitPerGroup,
            int groupCount,
            int pagesProcessed,
            long rowsReceived,
            long rowsEmitted,
            long ramBytesUsed,
            long rowsSpilled,
            int partitionsReplayed,
            SpillStatus spill
        ) {
            this.limitPerGroup = limitPerGroup;
            this.groupCount = groupCount;
            this.pagesProcessed = pagesProcessed;
            this.rowsReceived = rowsReceived;
            this.rowsEmitted = rowsEmitted;
            this.ramBytesUsed = ramBytesUsed;
            this.rowsSpilled = rowsSpilled;
            this.partitionsReplayed = partitionsReplayed;
            this.spill = spill;
        }

        protected Status(StreamInput in) throws IOException {
            limitPerGroup = in.readVInt();
            groupCount = in.readVInt();
            pagesProcessed = in.readVInt();
            rowsReceived = in.readVLong();
            rowsEmitted = in.readVLong();
            ramBytesUsed = in.readVLong();
            if (in.getTransportVersion().supports(SpillStatus.ESQL_SPILL_STATUS)) {
                rowsSpilled = in.readVLong();
                partitionsReplayed = in.readVInt();
            } else {
                rowsSpilled = 0;
                partitionsReplayed = 0;
            }
            spill = SpillStatus.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(limitPerGroup);
            out.writeVInt(groupCount);
            out.writeVInt(pagesProcessed);
            out.writeVLong(rowsReceived);
            out.writeVLong(rowsEmitted);
            out.writeVLong(ramBytesUsed);
            if (out.getTransportVersion().supports(SpillStatus.ESQL_SPILL_STATUS)) {
                out.writeVLong(rowsSpilled);
                out.writeVInt(partitionsReplayed);
            }
            spill.writeTo(out);
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

        public long rowsSpilled() {
            return rowsSpilled;
        }

        public int partitionsReplayed() {
            return partitionsReplayed;
        }

        public SpillStatus spill() {
            return spill;
        }

        private XContentBuilder writeProfile(XContentBuilder builder) throws IOException {
            builder.startObject();
            builder.field("limit_per_group", limitPerGroup);
            builder.field("group_count", groupCount);
            builder.field("pages_processed", pagesProcessed);
            builder.field("rows_received", rowsReceived);
            builder.field("rows_emitted", rowsEmitted);
            builder.field("ram_bytes_used", ramBytesUsed);
            builder.field("ram_used", ByteSizeValue.ofBytes(ramBytesUsed));
            // present by value: an operator that never froze writes none of these
            if (rowsSpilled > 0 || partitionsReplayed > 0 || spill.isNone() == false) {
                builder.field("rows_spilled", rowsSpilled);
                builder.field("partitions_replayed", partitionsReplayed);
                spill.toXContent(builder, ToXContent.EMPTY_PARAMS);
            }
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
            return limitPerGroup == status.limitPerGroup
                && groupCount == status.groupCount
                && pagesProcessed == status.pagesProcessed
                && rowsReceived == status.rowsReceived
                && rowsEmitted == status.rowsEmitted
                && ramBytesUsed == status.ramBytesUsed
                && rowsSpilled == status.rowsSpilled
                && partitionsReplayed == status.partitionsReplayed
                && spill.equals(status.spill);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                limitPerGroup,
                groupCount,
                pagesProcessed,
                rowsReceived,
                rowsEmitted,
                ramBytesUsed,
                rowsSpilled,
                partitionsReplayed,
                spill
            );
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
