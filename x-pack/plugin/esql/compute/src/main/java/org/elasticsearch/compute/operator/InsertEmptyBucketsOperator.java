/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.IntroSorter;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SequencedMap;

/**
 * Fills in the empty buckets of an aggregation grouped by {@code BUCKET(..., {"include_empty_buckets": true})}.
 * <p>
 * This operator does the following:
 * <ul>
 *     <li>Collect all input pages (everything in memory, but the preceding agg did that too)</li>
 *     <li>Sort by groups, buckets (groups=the agg's groupings without include_empty_buckets, buckets=with include_empty_buckets)</li>
 *     <li>Create cursors that create all bucket values</li>
 *     <li>Walk through the input and cursors, and emit all empty and non-empty rows in order</li>
 *     <li>Output them in pages of max. size</li>
 * </ul>
 */
public class InsertEmptyBucketsOperator extends CompleteInputCollectorOperator {

    /**
     * @param bucketCursorFactories the grouping channels produced by include-empty {@code BUCKET}s, each mapped to a
     *                              {@link BucketCursorFactory} that creates the {@link BucketCursor} generating that bucket's boundaries.
     * @param groupChannels         the non-bucket grouping channels; a group is a distinct combination of their values.
     * @param defaultValues         the value (=not grouping) channel's default values for an empty bucket.
     */
    public record Factory(
        SequencedMap<Integer, BucketCursorFactory> bucketCursorFactories,
        List<Integer> groupChannels,
        Map<Integer, DefaultValue> defaultValues,
        int maxPageSize
    ) implements OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            SequencedMap<Integer, BucketCursor> bucketCursors = new LinkedHashMap<>();
            bucketCursorFactories.forEach((channel, factory) -> bucketCursors.put(channel, factory.create()));
            return new InsertEmptyBucketsOperator(driverContext, bucketCursors, groupChannels, defaultValues, maxPageSize);
        }

        @Override
        public String describe() {
            return "InsertEmptyBucketsOperator[bucketCursorFactories="
                + bucketCursorFactories.keySet()
                + ", groupChannels="
                + groupChannels
                + ", defaultValues="
                + defaultValues.keySet()
                + "]";
        }
    }

    /**
     * The default an empty bucket takes on a single output channel, plus that channel's element type.
     */
    public record DefaultValue(ElementType type, @Nullable Object value) {}

    /**
     * Creates fresh {@link BucketCursor} instances. Because cursors are stateful, each operator instance needs its own,
     * so the {@link Factory} holds these (stateless) factories rather than the cursors themselves.
     */
    public interface BucketCursorFactory {
        BucketCursor create();
    }

    /**
     * Walks the bucket boundaries of a single group in ascending order, one at a time. The boundaries of a
     * {@code from}..{@code to} range are never materialized (there can be astronomically many, e.g. a one-year range at
     * millisecond resolution), only the range and the step are held. Instances are stateful.
     */
    public interface BucketCursor {
        /** Type of the emitted elements */
        ElementType type();

        /** Position at the first boundary of the range. */
        void reset();

        /** True once every boundary in the range has been passed. */
        boolean exhausted();

        /** Move to the next boundary. */
        void advance();
    }

    public record DateCursorFactory(Rounding.Prepared rounding, long from, long to, boolean nanos) implements BucketCursorFactory {
        @Override
        public BucketCursor create() {
            return new DateCursor(rounding, from, to, nanos);
        }
    }

    public static final class DateCursor implements BucketCursor {
        private final Rounding.Prepared rounding;
        private final long from;
        private final long to;
        private final boolean nanos;
        private long current;

        public DateCursor(Rounding.Prepared rounding, long from, long to, boolean nanos) {
            this.rounding = rounding;
            this.from = from;
            this.to = to;
            this.nanos = nanos;
        }

        @Override
        public ElementType type() {
            return ElementType.LONG;
        }

        @Override
        public void reset() {
            current = rounding.round(from);
        }

        @Override
        public boolean exhausted() {
            return current >= to;
        }

        @Override
        public void advance() {
            current = rounding.nextRoundingValue(current);
        }

        public long currentLong() {
            return nanos ? DateUtils.toNanoSeconds(current) : current;
        }
    }

    public record NumericCursorFactory(double roundTo, double from, double to) implements BucketCursorFactory {
        @Override
        public BucketCursor create() {
            return new NumericCursor(roundTo, from, to);
        }
    }

    public static final class NumericCursor implements BucketCursor {
        private final double roundTo;
        private final double from;
        private final double to;
        private final boolean alwaysExhausted;
        private long n;

        public NumericCursor(double roundTo, double from, double to) {
            this.roundTo = roundTo;
            this.from = from;
            this.to = to;
            this.alwaysExhausted = from >= to || Double.isFinite(roundTo) == false || roundTo <= 0.0;
        }

        @Override
        public ElementType type() {
            return ElementType.DOUBLE;
        }

        @Override
        public void reset() {
            n = (long) Math.floor(from / roundTo);
        }

        @Override
        public boolean exhausted() {
            return roundTo * n >= to || alwaysExhausted;
        }

        @Override
        public void advance() {
            n++;
        }

        public double currentDouble() {
            return roundTo * n;
        }
    }

    private final DriverContext driverContext;
    private final SequencedMap<Integer, BucketCursor> bucketCursors;
    private final List<Integer> groupChannels;
    private final Map<Integer, DefaultValue> defaultValues;

    // Total number of output channels = group + bucket + value channels (a disjoint partition of [0, channelCount)).
    private final int channelCount;
    private final int maxPageSize;

    private final boolean[] isGroupChannel;
    private final BucketCursor[] cursorsByChannel;
    private final DefaultValue[] defaultValuesByChannel;

    private final int[] cursorChannels;    // The bucket cursors channels in the (sort) order of bucketCursors.
    private final BucketCursor[] cursors;  // The bucket cursors in the (sort) order of bucketCursors.

    // Compares two rows by the (non-bucket) grouping keys (to detect group boundaries).
    private final GroupKeyComparator groupKeyComparator;
    // Compares two rows by the full sort key (grouping keys + bucket keys) to order the buffered input.
    private final GroupKeyComparator sortKeyComparator;

    // All sorted input rows, constructed in onFinished.
    // Instead of copying all data, pointers in `sortedRowPointers` to each (page, position) are sorted.
    // The pointers are long values: the high bits are the page index, the low bits are the position in that page.
    private Page[] pages;
    private LongArray sortedRowPointers;

    // Walk state, over the sorted row pointers.
    private int nextRow;                   // the next real input row to emit
    private final Object[] nextRowValues;  // the next real input row's bucket values (indexed by cursor index; cached for efficiency)

    private int currentGroup = -1;     // representative row of the active group, used to copy grouping values (-1 if none)
    private Page currentGroupPage;     // page of the representative row of the active group
    private int currentGroupPosition;  // position of the representative row of the active group
    private boolean bucketCursorExhausted;
    private boolean inEmptyInputGroup;

    InsertEmptyBucketsOperator(
        DriverContext driverContext,
        SequencedMap<Integer, BucketCursor> bucketCursors,
        List<Integer> groupChannels,
        Map<Integer, DefaultValue> defaultValues,
        int maxPageSize
    ) {
        super();
        this.driverContext = driverContext;

        // Only used for toString.
        this.bucketCursors = bucketCursors;
        this.groupChannels = groupChannels;
        this.defaultValues = defaultValues;

        this.channelCount = groupChannels.size() + bucketCursors.size() + defaultValues.size();

        // Indexed by channel in small arrays for efficiency, because they're consulted for every emitted empty bucket.
        this.isGroupChannel = new boolean[channelCount];
        groupChannels.forEach(channel -> isGroupChannel[channel] = true);
        this.cursorsByChannel = new BucketCursor[channelCount];
        bucketCursors.forEach((channel, cursor) -> this.cursorsByChannel[channel] = cursor);
        this.defaultValuesByChannel = new DefaultValue[channelCount];
        defaultValues.forEach((channel, defaultValue) -> this.defaultValuesByChannel[channel] = defaultValue);
        this.cursors = bucketCursors.values().toArray(new BucketCursor[0]);
        this.cursorChannels = bucketCursors.keySet().stream().mapToInt(Integer::intValue).toArray();

        this.nextRowValues = new Object[channelCount];

        this.groupKeyComparator = new GroupKeyComparator(groupChannels);
        List<Integer> sortChannels = new ArrayList<>(groupChannels);
        sortChannels.addAll(bucketCursors.keySet());
        this.sortKeyComparator = new GroupKeyComparator(sortChannels);
        this.maxPageSize = maxPageSize;
    }

    @Override
    protected void onFinished() {
        // When all input is collected, sort pointers to each row (i.e. page + position).

        pages = inputPages.toArray(new Page[0]);
        int rowCount = 0;
        for (Page page : pages) {
            assert page.getBlockCount() == channelCount;
            try {
                rowCount = Math.addExact(rowCount, page.getPositionCount());
            } catch (ArithmeticException e) {
                throw new IllegalStateException("too many rows in InsertEmptyBucketsOperator");
            }
        }

        // With no input rows and no non-bucket groupings, the full BUCKET range is still emitted (a histogram of empty
        // buckets). With extra groupings there are no groups to enumerate, so nothing is emitted.
        inEmptyInputGroup = rowCount == 0 && groupChannels.isEmpty();
        if (inEmptyInputGroup) {
            // There is no real group to trigger it, so position the cursor at the range's first boundary here.
            resetBucketCursor();
        }

        sortedRowPointers = driverContext.bigArrays().newLongArray(rowCount, false);
        int idx = 0;
        for (int pageIndex = 0; pageIndex < pages.length; pageIndex++) {
            int positions = pages[pageIndex].getPositionCount();
            for (int pos = 0; pos < positions; pos++) {
                sortedRowPointers.set(idx++, (((long) pageIndex) << Integer.SIZE) | pos);
            }
        }

        new IntroSorter() {
            private long pivot;

            @Override
            protected void setPivot(int i) {
                pivot = sortedRowPointers.get(i);
            }

            @Override
            protected int comparePivot(int j) {
                return compareRows(pivot, sortedRowPointers.get(j));
            }

            @Override
            protected int compare(int i, int j) {
                return compareRows(sortedRowPointers.get(i), sortedRowPointers.get(j));
            }

            @Override
            protected void swap(int i, int j) {
                long tmp = sortedRowPointers.get(i);
                sortedRowPointers.set(i, sortedRowPointers.get(j));
                sortedRowPointers.set(j, tmp);
            }
        }.sort(0, rowCount);
    }

    private int compareRows(long rowA, long rowB) {
        return sortKeyComparator.compare(page(rowA), position(rowA), page(rowB), position(rowB));
    }

    private Page page(long row) {
        return pages[(int) (row >>> Integer.SIZE)];
    }

    private int position(long row) {
        return (int) (row & Integer.MAX_VALUE);
    }

    @Override
    protected boolean isOperatorFinished() {
        return currentGroup == -1 && nextRow >= sortedRowPointers.size() && inEmptyInputGroup == false;
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        return finished && isOperatorFinished() == false;
    }

    @Override
    protected Page onGetOutput() {
        Block.Builder[] builders = new Block.Builder[channelCount];
        try {
            BlockFactory blockFactory = driverContext.blockFactory();
            for (int c = 0; c < channelCount; c++) {
                builders[c] = elementType(c).newBlockBuilder(maxPageSize, blockFactory);
            }
            int rows = 0;
            while (rows < maxPageSize) {
                int addedRows = appendNextRows(builders, maxPageSize - rows);
                if (addedRows == 0) {
                    break;
                }
                rows += addedRows;
            }
            Block[] blocks = new Block[channelCount];
            try {
                for (int c = 0; c < channelCount; c++) {
                    blocks[c] = builders[c].build();
                }
                return new Page(blocks);
            } catch (Exception e) {
                Releasables.closeExpectNoException(blocks);
                throw e;
            }
        } finally {
            Releasables.closeExpectNoException(builders);
        }
    }

    /**
     * The element type of output channel {@code c}.
     */
    private ElementType elementType(int c) {
        BucketCursor cursor = cursorsByChannel[c];
        if (cursor != null) {
            return cursor.type();
        }
        DefaultValue defaultValue = defaultValuesByChannel[c];
        if (defaultValue != null) {
            return defaultValue.type();
        }
        return pages[0].getBlock(c).elementType();
    }

    @Override
    protected void onClose() {
        Releasables.close(sortedRowPointers);
    }

    @Override
    public String toString() {
        return "InsertEmptyBucketsOperator[bucketCursors="
            + bucketCursors.keySet()
            + ", groupChannels="
            + groupChannels
            + ", defaultValues="
            + defaultValues.keySet()
            + "]";
    }

    /**
     * Adds the next row(s) (either one input row or a number of empty buckets) to the builders.
     * @return the number of rows added
     */
    private int appendNextRows(Block.Builder[] builders, int maxRows) {
        if (inEmptyInputGroup) {
            if (bucketCursorExhausted == false) {
                int rows = 0;
                do {
                    appendEmptyBucket(builders);
                    advanceBucketCursor();
                    rows++;
                } while (bucketCursorExhausted == false && rows < maxRows);
                return rows;
            } else {
                // The synthetic group is fully emitted; the operator is now finished.
                inEmptyInputGroup = false;
                return 0;
            }
        }

        while (true) {
            if (currentGroup == -1) {
                // The previous group (if any) is completely processed.
                if (nextRow >= sortedRowPointers.size()) {
                    // All groups are finished.
                    return 0;
                } else {
                    // Start processing the next group.
                    currentGroup = nextRow;
                    long pointer = sortedRowPointers.get(currentGroup);
                    currentGroupPage = page(pointer);
                    currentGroupPosition = position(pointer);
                    resetBucketCursor();
                }
            }

            if (nextRow < sortedRowPointers.size() && compareGroupKeys(nextRow, currentGroup) == 0) {
                // The next input row is in the current group; insert either the next input bucket or the next cursor
                // bucket, depending on which should go first.
                cacheNextRowValues();
                int cmp = compareBucketCursorToNextRow();
                if (cmp < 0) {
                    // The next cursor bucket is smaller than the next input bucket: insert empty buckets up to it.
                    int rows = 0;
                    do {
                        appendEmptyBucket(builders);
                        advanceBucketCursor();
                        rows++;
                        cmp = compareBucketCursorToNextRow();
                    } while (cmp < 0 && rows < maxRows);
                    return rows;
                } else {
                    // The next input bucket is smaller than or equal to the next cursor bucket: insert the input bucket.
                    appendInputRow(builders, nextRow);
                    nextRow++;
                    if (cmp == 0) {
                        // The next input bucket equals the next cursor bucket, advance the cursor too.
                        advanceBucketCursor();
                    }
                    return 1;
                }
            }

            // The input buckets for the current group are processed; emit its remaining (trailing) empty buckets.
            if (bucketCursorExhausted == false) {
                int rows = 0;
                do {
                    appendEmptyBucket(builders);
                    advanceBucketCursor();
                    rows++;
                } while (bucketCursorExhausted == false && rows < maxRows);
                return rows;
            } else {
                // The group is fully processed; reset and try again (the next iteration starts the next group).
                currentGroup = -1;
            }
        }
    }

    private void appendInputRow(Block.Builder[] builders, int row) {
        long pointer = sortedRowPointers.get(row);
        Page page = page(pointer);
        int pos = position(pointer);
        for (int c = 0; c < page.getBlockCount(); c++) {
            builders[c].copyFrom(page.getBlock(c), pos, pos + 1);
        }
    }

    private void appendEmptyBucket(Block.Builder[] builders) {
        // In the empty-input synthetic group there is no representative row (and no group channels), so no page is read.
        for (int c = 0; c < channelCount; c++) {
            Block.Builder builder = builders[c];
            BucketCursor cursor;
            if (isGroupChannel[c]) {
                // Preserve grouping values.
                builder.copyFrom(currentGroupPage.getBlock(c), currentGroupPosition, currentGroupPosition + 1);
            } else if ((cursor = cursorsByChannel[c]) != null) {
                // Add next cursor bucket to bucket channels.
                switch (cursor) {
                    case DateCursor dateCursor -> ((LongBlock.Builder) builder).appendLong(dateCursor.currentLong());
                    case NumericCursor numericCursor -> ((DoubleBlock.Builder) builder).appendDouble(numericCursor.currentDouble());
                    default -> throw new IllegalArgumentException("unexpected bucket cursor [" + cursor.getClass() + "]");
                }
            } else {
                Object value = defaultValuesByChannel[c].value();
                if (value == null) {
                    builder.appendNull();
                } else {
                    assert defaultValuesByChannel[c].type() == ElementType.LONG : "default values only allowed for long values";
                    ((LongBlock.Builder) builder).appendLong((Long) value);
                }
            }
        }
    }

    private int compareGroupKeys(int rowA, int rowB) {
        long a = sortedRowPointers.get(rowA);
        long b = sortedRowPointers.get(rowB);
        return groupKeyComparator.compare(page(a), position(a), page(b), position(b));
    }

    private void cacheNextRowValues() {
        long pointer = sortedRowPointers.get(nextRow);
        Page page = page(pointer);
        int pos = position(pointer);
        for (int i = 0; i < cursors.length; i++) {
            Block block = page.getBlock(cursorChannels[i]);
            int valueIndex = block.getFirstValueIndex(pos);
            if (block.isNull(valueIndex)) {
                nextRowValues[i] = null;
            } else {
                nextRowValues[i] = switch (block.elementType()) {
                    case LONG -> ((LongBlock) block).getLong(valueIndex);
                    case DOUBLE -> ((DoubleBlock) block).getDouble(valueIndex);
                    default -> throw new IllegalArgumentException("unexpected element type [" + block.elementType() + "]");
                };
            }
        }
    }

    private int compareBucketCursorToNextRow() {
        if (bucketCursorExhausted) {
            // If the cursor is exhausted, report that the next cursor bucket is larger,
            // so the input bucket gets added instead of the (non-existing) cursor bucket.
            return 1;
        }
        for (int i = 0; i < cursors.length; i++) {
            BucketCursor cursor = cursors[i];
            if (nextRowValues[i] == null) {
                // If the input bucket is null, report that the cursor bucket is larger,
                // so that the cursor bucket gets added and the null comes at the end.
                return -1;
            }
            int cmp = switch (cursor) {
                case DateCursor dateCursor -> Long.compare(dateCursor.currentLong(), (Long) nextRowValues[i]);
                case NumericCursor numCursor -> Double.compare(numCursor.currentDouble(), (Double) nextRowValues[i]);
                default -> throw new IllegalArgumentException("unexpected bucket cursor [" + cursor.getClass() + "]");
            };
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    private void resetBucketCursor() {
        bucketCursorExhausted = false;
        if (cursors.length == 0) {
            bucketCursorExhausted = true;
            return;
        }
        for (BucketCursor cursor : cursors) {
            cursor.reset();
            if (cursor.exhausted()) {
                bucketCursorExhausted = true;
                return;
            }
        }
    }

    private void advanceBucketCursor() {
        if (bucketCursorExhausted) {
            return;
        }
        for (int i = cursors.length - 1; i >= 0; i--) {
            BucketCursor cursor = cursors[i];
            cursor.advance();
            if (cursor.exhausted() == false) {
                return;
            }
            cursor.reset();
            if (cursor.exhausted()) {
                bucketCursorExhausted = true;
                return;
            }
        }
        bucketCursorExhausted = true;
    }
}
