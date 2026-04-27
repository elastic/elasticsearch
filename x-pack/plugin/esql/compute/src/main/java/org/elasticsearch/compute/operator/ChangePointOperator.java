/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.ml.aggs.MlAggsHelper;
import org.elasticsearch.xpack.ml.aggs.changepoint.ChangePointDetector;
import org.elasticsearch.xpack.ml.aggs.changepoint.ChangeType;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;

/**
 * Find spikes, dips and change points in a list of values.
 * <p>
 * In grouped mode the operator streams output per group: as soon as a group
 * boundary is detected in the grouped input, the completed group is
 * flushed through the change-point detector, annotated, and queued for output.
 * <p>
 * In non-grouped mode all input is buffered until {@code finish()} because
 * the single implicit group cannot complete earlier.
 * <p>
 * Warning: even in grouped mode the operator cannot handle large groups! It
 * buffers all data for the current group, runs the change point detector
 * (which is a compute-heavy process), and then outputs the annotated data.
 */
public class ChangePointOperator implements Operator {
    private static final Logger logger = LogManager.getLogger(ChangePointOperator.class);
    public static final int INPUT_VALUE_COUNT_LIMIT = 1000;

    public record Factory(int channel, List<Integer> groupingChannels, WarningSourceLocation source) implements OperatorFactory {

        @Override
        public Operator get(DriverContext driverContext) {
            int[] channels = groupingChannels.stream().mapToInt(Integer::intValue).toArray();
            return new ChangePointOperator(driverContext, channel, channels, source);
        }

        @Override
        public String describe() {
            return Strings.format("ChangePointOperator[channel=%d, groupingChannels=%s]", channel, groupingChannels);
        }
    }

    private final DriverContext driverContext;
    private final int channel;
    private final int[] groupingChannels;
    private final WarningSourceLocation source;

    // Group tracking: each buffered page belongs wholly to the currently-open group.
    // Pages spanning a group boundary are split via Page#slice.
    private GroupKeyEncoder encoder;
    private BytesRef currentGroupKey;
    private final Deque<Page> currentGroupPages;

    private final Deque<Page> outputPages;
    private boolean finished;

    // Warning flags (accumulated across all groups)
    private boolean hasNulls;
    private boolean hasMultivalued;
    private boolean hasIndeterminableChangePoint;
    private boolean tooManyValues;
    private String indeterminableChangePointReason = "";
    private Warnings warnings;

    public ChangePointOperator(DriverContext driverContext, int channel, int[] groupingChannels, WarningSourceLocation source) {
        this.driverContext = driverContext;
        this.channel = channel;
        this.groupingChannels = groupingChannels;
        this.source = source;

        this.currentGroupPages = new ArrayDeque<>();

        this.outputPages = new ArrayDeque<>();
        this.finished = false;
    }

    @Override
    public boolean needsInput() {
        return finished == false && outputPages.isEmpty();
    }

    @Override
    public void addInput(Page page) {
        try {
            processPage(page);
        } catch (Exception e) {
            page.releaseBlocks();
            throw e;
        }
    }

    @Override
    public void finish() {
        if (finished == false) {
            finished = true;
            // Always flush, even with an empty buffer, so the "not enough buckets"
            // indeterminate-warning path runs regardless of grouped/non-grouped mode.
            flushGroup();
            emitWarnings();
        }
    }

    @Override
    public boolean isFinished() {
        return finished && outputPages.isEmpty();
    }

    @Override
    public Page getOutput() {
        if (outputPages.isEmpty()) {
            return null;
        }
        return outputPages.removeFirst();
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        return outputPages.isEmpty() == false;
    }

    @Override
    public void close() {
        Releasables.close(() -> Releasables.close(currentGroupPages), () -> Releasables.close(outputPages), encoder);
    }

    @Override
    public String toString() {
        return "ChangePointOperator[channel=" + channel + ", groupingChannels=" + Arrays.toString(groupingChannels) + "]";
    }

    /**
     * Accepts a page and routes its rows into {@link #currentGroupPages}, invoking
     * change-point detection whenever a group boundary is crossed.
     * <p>
     * In grouped mode a page that spans multiple groups is sliced into per-group
     * segments via {@link Page#slice}, so every buffered page belongs
     * wholly to a single group. In non-grouped mode every page is buffered for
     * the single implicit group that is flushed on {@link #finish()}.
     */
    private void processPage(Page page) {
        if (groupingChannels.length > 0 && encoder == null) {
            initEncoder(page);
            currentGroupKey = BytesRef.deepCopyOf(encoder.encode(page, 0));
        }

        if (groupingChannels.length == 0) {
            currentGroupPages.add(page);
            return;
        }

        int positionCount = page.getPositionCount();
        int scanStart = 0;
        for (int i = 0; i < positionCount; i++) {
            BytesRef key = encoder.encode(page, i);
            if (key.equals(currentGroupKey)) {
                continue;
            }
            // Group boundary at position i — slice off the completed segment and flush.
            if (i > scanStart) {
                currentGroupPages.add(page.slice(scanStart, i));
            }
            flushGroup();
            scanStart = i;
            currentGroupKey = BytesRef.deepCopyOf(key);
        }

        if (scanStart == 0) {
            // Whole page belongs to the currently-open group
            currentGroupPages.add(page);
        } else {
            // We had at least one boundary, buffer the remainder of the page
            currentGroupPages.add(page.slice(scanStart, positionCount));
            page.releaseBlocks();
        }
    }

    private void initEncoder(Page page) {
        List<ElementType> elementTypes = new ArrayList<>(page.getBlockCount());
        for (int i = 0; i < page.getBlockCount(); i++) {
            elementTypes.add(page.getBlock(i).elementType());
        }
        var scratch = new BreakingBytesRefBuilder(driverContext.blockFactory().breaker(), "change-point-group-key");
        encoder = new GroupKeyEncoder(groupingChannels, elementTypes, scratch);
    }

    /**
     * Runs change-point detection over the pages buffered in {@link #currentGroupPages}
     * (which all belong to a single, now-completed group), annotates each page with the
     * change-type / p-value columns, and moves them to the output queue.
     */
    private void flushGroup() {
        // 1. Collect values across the group's pages.
        List<Double> values = new ArrayList<>();
        List<Integer> bucketIndexes = new ArrayList<>();
        int groupRowIndex = 0;
        for (Page page : currentGroupPages) {
            groupRowIndex = accumulateValues(page, values, bucketIndexes, groupRowIndex);
        }

        // 2. Detect change point
        ChangeType changeType = detectChangePoint(values, bucketIndexes);
        int changePointIndex = changeType.changePoint(); // group-local row index, or -1
        if (changeType instanceof ChangeType.Indeterminable indeterminable && hasIndeterminableChangePoint == false) {
            hasIndeterminableChangePoint = true;
            indeterminableChangePointReason = indeterminable.getReason();
        }

        // 3. Annotate and emit pages
        int cumulativeRows = 0;
        while (currentGroupPages.isEmpty() == false) {
            Page page = currentGroupPages.peekFirst();
            int pageCpPos = -1;
            if (changePointIndex >= 0
                && changePointIndex >= cumulativeRows
                && changePointIndex < cumulativeRows + page.getPositionCount()) {
                pageCpPos = changePointIndex - cumulativeRows;
            }
            Page annotated = annotatePageWithChangePoint(page, pageCpPos, changeType);
            currentGroupPages.removeFirst();
            outputPages.add(annotated);
            cumulativeRows += page.getPositionCount();
        }
    }

    /**
     * Extracts values from {@code page} and appends them to the provided lists.
     * Updates warning flags for nulls, multivalued entries, and the per-group
     * value count limit.
     *
     * @return the updated {@code groupRowIndex}
     */
    private int accumulateValues(Page page, List<Double> values, List<Integer> bucketIndexes, int groupRowIndex) {
        Block inputBlock = page.getBlock(channel);
        int positionCount = page.getPositionCount();
        for (int i = 0; i < positionCount; i++) {
            if (groupRowIndex >= INPUT_VALUE_COUNT_LIMIT) {
                // Past the limit, no further rows are added; account for the rest in one shot.
                tooManyValues = true;
                return groupRowIndex + (positionCount - i);
            }

            Object value = BlockUtils.toJavaObject(inputBlock, i);
            if (value == null) {
                hasNulls = true;
                groupRowIndex++;
            } else if (value instanceof List<?>) {
                hasMultivalued = true;
                groupRowIndex++;
            } else {
                values.add(((Number) value).doubleValue());
                bucketIndexes.add(groupRowIndex++);
            }
        }
        return groupRowIndex;
    }

    private ChangeType detectChangePoint(List<Double> values, List<Integer> bucketIndexes) {
        MlAggsHelper.DoubleBucketValues bucketValues = new MlAggsHelper.DoubleBucketValues(
            null,
            values.stream().mapToDouble(Double::doubleValue).toArray(),
            bucketIndexes.stream().mapToInt(Integer::intValue).toArray()
        );
        return ChangePointDetector.getChangeType(bucketValues);
    }

    /**
     * Appends change_type and change_pvalue columns to the page. When
     * {@code changePointPosition >= 0}, that position is annotated with
     * {@code changeType}; all other positions are null. A negative position
     * means the page contains no change point.
     */
    private Page annotatePageWithChangePoint(Page page, int changePointPosition, ChangeType changeType) {
        BlockFactory blockFactory = driverContext.blockFactory();
        Block changeTypeBlock = null;
        Block changePvalueBlock = null;
        boolean success = false;
        try {
            if (changePointPosition < 0) {
                changeTypeBlock = blockFactory.newConstantNullBlock(page.getPositionCount());
                changePvalueBlock = blockFactory.newConstantNullBlock(page.getPositionCount());
            } else {
                try (
                    BytesRefBlock.Builder typeBuilder = blockFactory.newBytesRefBlockBuilder(page.getPositionCount());
                    DoubleBlock.Builder pvalueBuilder = blockFactory.newDoubleBlockBuilder(page.getPositionCount())
                ) {
                    for (int i = 0; i < page.getPositionCount(); i++) {
                        if (i == changePointPosition) {
                            typeBuilder.appendBytesRef(new BytesRef(changeType.getWriteableName()));
                            pvalueBuilder.appendDouble(changeType.pValue());
                        } else {
                            typeBuilder.appendNull();
                            pvalueBuilder.appendNull();
                        }
                    }
                    changeTypeBlock = typeBuilder.build();
                    changePvalueBlock = pvalueBuilder.build();
                }
            }
            Page result = page.appendBlocks(new Block[] { changeTypeBlock, changePvalueBlock });
            success = true;
            return result;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(changeTypeBlock, changePvalueBlock);
            }
        }
    }

    private void emitWarnings() {
        if (tooManyValues) {
            logger.debug(() -> Strings.format("Too many values: limit is %d, some values were ignored", INPUT_VALUE_COUNT_LIMIT));
            warnings(true).registerException(
                new IllegalArgumentException("too many values; keeping only first " + INPUT_VALUE_COUNT_LIMIT + " values")
            );
        }
        if (hasIndeterminableChangePoint) {
            logger.debug(() -> Strings.format("Change point indeterminable: %s", indeterminableChangePointReason));
            warnings(false).registerException(new IllegalArgumentException(indeterminableChangePointReason));
        }
        if (hasNulls) {
            logger.debug(() -> "Values contain nulls; skipping them");
            warnings(true).registerException(new IllegalArgumentException("values contain nulls; skipping them"));
        }
        if (hasMultivalued) {
            logger.debug(() -> "Values contain multivalued entries; skipping them");
            warnings(true).registerException(
                new IllegalArgumentException(
                    "values contains multivalued entries; skipping them (please consider reducing them with e.g. MV_AVG or MV_SUM)"
                )
            );
        }
    }

    private Warnings warnings(boolean onlyWarnings) {
        if (warnings == null) {
            if (onlyWarnings) {
                this.warnings = Warnings.createOnlyWarnings(driverContext.warningsMode(), source);
            } else {
                this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
            }
        }
        return warnings;
    }
}
