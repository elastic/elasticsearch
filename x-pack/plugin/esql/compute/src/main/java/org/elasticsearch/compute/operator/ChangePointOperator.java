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
 * boundary is detected in the (pre-sorted) input, the completed group is
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

    private record ChangePointAnnotation(int positionInPage, ChangeType changeType) {}

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

    // Group tracking
    private GroupKeyEncoder encoder;
    private BytesRef currentGroupKey;
    private final Deque<Page> currentGroupPages;

    // Boundary page: a page that spans two or more groups.
    // Held until all groups it contains have completed detection.
    private Page pendingBoundaryPage;
    private int pendingBoundaryGroupStart;
    private List<ChangePointAnnotation> pendingBoundaryChangePoints;

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
            // Flush the last (or only) group
            if (groupingChannels.length == 0 || currentGroupPages.isEmpty() == false || pendingBoundaryPage != null) {
                flushGroup(null, null, -1, -1);
            }
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
        Releasables.close(() -> Releasables.close(currentGroupPages), () -> {
            if (pendingBoundaryPage != null) {
                pendingBoundaryPage.releaseBlocks();
                pendingBoundaryPage = null;
            }
        }, () -> Releasables.close(outputPages), encoder);
    }

    @Override
    public String toString() {
        return "ChangePointOperator[channel=" + channel + ", groupingChannels=" + Arrays.toString(groupingChannels) + "]";
    }

    /**
     * Accepts a page, invokes changepoint detection on group boundary change
     * (in grouped mode) or buffers it for later.
     * <p>
     * In non-grouped mode the encoder is null and the boundary-detection loop
     * is skipped entirely — every page is simply buffered for the single
     * implicit group that is flushed on {@link #finish()}.
     */
    private void processPage(Page page) {
        // Lazily initialise the group-key encoder on the first grouped page
        if (groupingChannels.length > 0 && encoder == null) {
            initEncoder(page);
            currentGroupKey = BytesRef.deepCopyOf(encoder.encode(page, 0));
        }

        // Detect group boundaries and flush completed groups
        int scanStart = 0;
        List<ChangePointAnnotation> pageChangePoints = null;

        if (encoder != null) {
            for (int i = 0; i < page.getPositionCount(); i++) {
                BytesRef key = encoder.encode(page, i);
                if (key.equals(currentGroupKey)) {
                    continue;
                }
                // Group boundary at position i — flush the completed group.
                if (pageChangePoints == null) {
                    pageChangePoints = new ArrayList<>();
                }
                flushGroup(page, pageChangePoints, scanStart, i);
                scanStart = i;
                currentGroupKey = BytesRef.deepCopyOf(key);
            }
        }

        // Track the page
        if (pageChangePoints == null || scanStart == 0) {
            // No boundary, or boundary only at position 0 — entire page is current group
            currentGroupPages.add(page);
        } else {
            // Page has boundary(ies) — hold it until the last group it contains completes
            pendingBoundaryPage = page;
            pendingBoundaryGroupStart = scanStart;
            pendingBoundaryChangePoints = pageChangePoints;
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
     * Collects values from accumulated pages, runs change-point detection, then
     * annotates and emits all pages belonging to that group.
     */
    private void flushGroup(Page newBoundaryPage, List<ChangePointAnnotation> newBoundaryChangePoints, int spanStart, int spanEnd) {
        // 1. Collect values from the group's pages
        List<Double> values = new ArrayList<>();
        List<Integer> bucketIndexes = new ArrayList<>();
        int groupRowIndex = 0;
        int groupRowCount = 0;

        if (pendingBoundaryPage != null) {
            int pendingSpanEnd = pendingBoundaryPage.getPositionCount();
            groupRowIndex = accumulateValues(
                pendingBoundaryPage,
                pendingBoundaryGroupStart,
                pendingSpanEnd,
                values,
                bucketIndexes,
                groupRowIndex,
                groupRowCount
            );
            groupRowCount += pendingSpanEnd - pendingBoundaryGroupStart;
        }
        for (Page page : currentGroupPages) {
            groupRowIndex = accumulateValues(page, 0, page.getPositionCount(), values, bucketIndexes, groupRowIndex, groupRowCount);
            groupRowCount += page.getPositionCount();
        }
        if (newBoundaryPage != null && spanStart < spanEnd) {
            accumulateValues(newBoundaryPage, spanStart, spanEnd, values, bucketIndexes, groupRowIndex, groupRowCount);
        }

        // 2. Detect change point
        ChangeType changeType = detectChangePoint(values, bucketIndexes);
        int changePointIndex = changeType.changePoint(); // group-local row index, or -1

        if (changeType instanceof ChangeType.Indeterminable indeterminable) {
            hasIndeterminableChangePoint = true;
            indeterminableChangePointReason = indeterminable.getReason();
        }

        // 3. Annotate and emit pages in input order
        int cumulativeRows = 0;

        // 3a. Resolve the pending boundary page (its trailing rows belonged to this now-completed group).
        if (pendingBoundaryPage != null) {
            int spanRows = pendingBoundaryPage.getPositionCount() - pendingBoundaryGroupStart;
            if (changePointIndex >= 0 && changePointIndex < spanRows) {
                pendingBoundaryChangePoints.add(new ChangePointAnnotation(pendingBoundaryGroupStart + changePointIndex, changeType));
            }
            outputPages.add(annotatePageWithChangePoints(pendingBoundaryPage, pendingBoundaryChangePoints));
            pendingBoundaryPage = null;
            pendingBoundaryChangePoints = null;
            cumulativeRows += spanRows;
        }

        // 3b. Annotate and emit all pages fully within this group.
        while (currentGroupPages.isEmpty() == false) {
            Page page = currentGroupPages.peekFirst();
            int pageCpPos = -1;
            if (changePointIndex >= 0 && changePointIndex >= cumulativeRows && changePointIndex < cumulativeRows + page.getPositionCount()) {
                pageCpPos = changePointIndex - cumulativeRows;
            }
            Page annotated = annotatePageWithChangePoints(
                page,
                pageCpPos >= 0 ? List.of(new ChangePointAnnotation(pageCpPos, changeType)) : List.of()
            );
            currentGroupPages.removeFirst();
            outputPages.add(annotated);
            cumulativeRows += page.getPositionCount();
        }

        // 3c. Record change point on the new boundary page (if it falls in this group's leading rows).
        if (newBoundaryPage != null && spanStart < spanEnd) {
            int spanRows = spanEnd - spanStart;
            if (changePointIndex >= 0 && changePointIndex >= cumulativeRows && changePointIndex < cumulativeRows + spanRows) {
                newBoundaryChangePoints.add(new ChangePointAnnotation(spanStart + (changePointIndex - cumulativeRows), changeType));
            }
        }
    }

    /**
     * Extracts values from a range of rows in a page and appends them to the
     * provided lists. Updates warning flags for nulls, multivalued entries,
     * and the per-group value count limit.
     *
     * @return the updated {@code groupRowIndex}
     */
    private int accumulateValues(
        Page page,
        int startPos,
        int endPos,
        List<Double> values,
        List<Integer> bucketIndexes,
        int groupRowIndex,
        int groupRowCount
    ) {
        Block inputBlock = page.getBlock(channel);
        for (int i = startPos; i < endPos; i++) {
            if (groupRowCount >= INPUT_VALUE_COUNT_LIMIT) {
                tooManyValues = true;
                groupRowIndex++;
                groupRowCount++;
                continue;
            }

            Object value = BlockUtils.toJavaObject(inputBlock, i);
            groupRowCount++;
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
     * Appends two columns (change_type, change_pvalue) to the page.
     * Positions listed in {@code changePoints} get the detected type/pvalue;
     * all other positions get null.
     */
    private Page annotatePageWithChangePoints(Page page, List<ChangePointAnnotation> changePoints) {
        BlockFactory blockFactory = driverContext.blockFactory();
        Block changeTypeBlock = null;
        Block changePvalueBlock = null;
        boolean success = false;
        try {
            if (changePoints.isEmpty()) {
                changeTypeBlock = blockFactory.newConstantNullBlock(page.getPositionCount());
                changePvalueBlock = blockFactory.newConstantNullBlock(page.getPositionCount());
            } else {
                try (
                    BytesRefBlock.Builder typeBuilder = blockFactory.newBytesRefBlockBuilder(page.getPositionCount());
                    DoubleBlock.Builder pvalueBuilder = blockFactory.newDoubleBlockBuilder(page.getPositionCount())
                ) {
                    int cpIdx = 0;
                    for (int i = 0; i < page.getPositionCount(); i++) {
                        if (cpIdx < changePoints.size() && i == changePoints.get(cpIdx).positionInPage()) {
                            ChangePointAnnotation cp = changePoints.get(cpIdx);
                            typeBuilder.appendBytesRef(new BytesRef(cp.changeType().getWriteableName()));
                            pvalueBuilder.appendDouble(cp.changeType().pValue());
                            cpIdx++;
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
