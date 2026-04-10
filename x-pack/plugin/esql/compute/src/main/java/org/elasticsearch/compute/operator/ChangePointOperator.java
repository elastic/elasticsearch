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
import org.elasticsearch.compute.expression.ExpressionEvaluator;
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
 * Find spikes, dips and change point in a list of values.
 * <p>
 * Warning: this operator cannot handle large amounts of data! It buffers all
 * data that is passed to it, runs the change point detector on the data (which
 * is a compute-heavy process), and then outputs all data with the change points.
 */
public class ChangePointOperator extends CompleteInputCollectorOperator {
    private static final Logger logger = LogManager.getLogger(ChangePointOperator.class);
    public static final int INPUT_VALUE_COUNT_LIMIT = 1000;

    private record DetectedChangePoint(int index, ChangeType type) {}

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

    private final Deque<Page> outputPages;
    private Warnings warnings;

    public ChangePointOperator(DriverContext driverContext, int channel, int[] groupingChannels, WarningSourceLocation source) {
        this.driverContext = driverContext;
        this.channel = channel;
        this.groupingChannels = groupingChannels;
        this.source = source;

        outputPages = new ArrayDeque<>();
        warnings = null;
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        return outputPages.isEmpty() == false;
    }

    private ChangeType detectChangePoint(List<Double> values, List<Integer> bucketIndexes) {
        MlAggsHelper.DoubleBucketValues bucketValues = new MlAggsHelper.DoubleBucketValues(
            null,
            values.stream().mapToDouble(Double::doubleValue).toArray(),
            bucketIndexes.stream().mapToInt(Integer::intValue).toArray()
        );
        ChangeType changeType = ChangePointDetector.getChangeType(bucketValues);
        return changeType;
    }

    private void createOutputPages() {
        List<Double> values = new ArrayList<>();
        List<Integer> bucketIndexes = new ArrayList<>();
        ArrayDeque<DetectedChangePoint> detectedChangePoints = new ArrayDeque<>();
        int valuesIndex = 0;
        int currentGroupRowCount = 0;

        GroupKeyEncoder encoder = null;
        BytesRef previousGroupKey = null;
        if (groupingChannels.length > 0 && inputPages.isEmpty() == false) {
            Page firstPage = inputPages.peek();
            List<ElementType> elementTypes = new ArrayList<>(firstPage.getBlockCount());
            for (int i = 0; i < firstPage.getBlockCount(); i++) {
                elementTypes.add(firstPage.getBlock(i).elementType());
            }
            var scratch = new BreakingBytesRefBuilder(driverContext.blockFactory().breaker(), "change-point-group-key");
            encoder = new GroupKeyEncoder(groupingChannels, elementTypes, scratch);
            previousGroupKey = BytesRef.deepCopyOf(encoder.encode(firstPage, 0));
        }

        boolean hasNulls = false;
        boolean hasMultivalued = false;
        boolean hasIndeterminableChangePoint = false;
        boolean tooManyValues = false;
        boolean lastGroupHasRows = false;
        String indeterminableChangePointReason = "";
        try {
            for (Page inputPage : inputPages) {
                Block inputBlock = inputPage.getBlock(channel);
                for (int i = 0; i < inputBlock.getPositionCount(); i++) {
                    if (encoder != null) {
                        BytesRef currentGroupKey = encoder.encode(inputPage, i);
                        if (currentGroupKey.equals(previousGroupKey) == false) {
                            if (values.isEmpty() == false || lastGroupHasRows) {
                                var changeType = detectChangePoint(values, bucketIndexes);
                                var changePointIndex = changeType.changePoint();
                                if (changePointIndex >= 0) {
                                    detectedChangePoints.offer(new DetectedChangePoint(changePointIndex, changeType));
                                }
                                if (changeType instanceof ChangeType.Indeterminable indeterminable) {
                                    hasIndeterminableChangePoint = true;
                                    indeterminableChangePointReason = indeterminable.getReason();
                                }
                                values.clear();
                                bucketIndexes.clear();
                                lastGroupHasRows = false;
                            }
                            previousGroupKey = BytesRef.deepCopyOf(currentGroupKey);
                            currentGroupRowCount = 0;
                        }
                    }

                    if (currentGroupRowCount >= INPUT_VALUE_COUNT_LIMIT) {
                        tooManyValues = true;
                        valuesIndex++;
                        continue;
                    }

                    Object value = BlockUtils.toJavaObject(inputBlock, i);
                    lastGroupHasRows = true;
                    currentGroupRowCount++;
                    if (value == null) {
                        hasNulls = true;
                        valuesIndex++;
                    } else if (value instanceof List<?>) {
                        hasMultivalued = true;
                        valuesIndex++;
                    } else {
                        values.add(((Number) value).doubleValue());
                        bucketIndexes.add(valuesIndex++);
                    }
                }
            }

            // flush last (or only) group; for "non-grouped" or "all-null" input this still
            // runs to produce an "indeterminable" warning.
            // Use groupingChannels.length == 0 (not encoder == null) to test for non-grouped mode:
            // encoder is also null in grouped mode when inputPages is empty, which must not trigger the flush.
            if (values.isEmpty() == false || groupingChannels.length == 0 || lastGroupHasRows) {
                var changeType = detectChangePoint(values, bucketIndexes);
                var changePointIndex = changeType.changePoint();
                if (changePointIndex >= 0) {
                    detectedChangePoints.offer(new DetectedChangePoint(changePointIndex, changeType));
                }
                if (changeType instanceof ChangeType.Indeterminable indeterminable) {
                    hasIndeterminableChangePoint = true;
                    indeterminableChangePointReason = indeterminable.getReason();
                }
            }
        } finally {
            Releasables.closeExpectNoException(encoder);
        }

        buildOutputPages(detectedChangePoints);
        emitWarnings(tooManyValues, hasNulls, hasMultivalued, hasIndeterminableChangePoint, indeterminableChangePointReason);
    }

    private void buildOutputPages(ArrayDeque<DetectedChangePoint> detectedChangePoints) {
        BlockFactory blockFactory = driverContext.blockFactory();
        int pageStartIndex = 0;
        while (inputPages.isEmpty() == false) {
            Page inputPage = inputPages.peek();
            int pageEndIndex = pageStartIndex + inputPage.getPositionCount();
            Page outputPage;
            Block changeTypeBlock = null;
            Block changePvalueBlock = null;
            boolean success = false;
            try {
                DetectedChangePoint head = detectedChangePoints.peek();
                if (head != null && head.index() < pageEndIndex) {
                    try (
                        BytesRefBlock.Builder changeTypeBlockBuilder = blockFactory.newBytesRefBlockBuilder(inputPage.getPositionCount());
                        DoubleBlock.Builder pvalueBlockBuilder = blockFactory.newDoubleBlockBuilder(inputPage.getPositionCount())
                    ) {
                        for (int i = 0; i < inputPage.getPositionCount(); i++) {
                            if (head != null && pageStartIndex + i == head.index()) {
                                changeTypeBlockBuilder.appendBytesRef(new BytesRef(head.type().getWriteableName()));
                                pvalueBlockBuilder.appendDouble(head.type().pValue());
                                detectedChangePoints.poll();
                                head = detectedChangePoints.peek();
                            } else {
                                changeTypeBlockBuilder.appendNull();
                                pvalueBlockBuilder.appendNull();
                            }
                        }
                        changeTypeBlock = changeTypeBlockBuilder.build();
                        changePvalueBlock = pvalueBlockBuilder.build();
                    }
                } else {
                    changeTypeBlock = blockFactory.newConstantNullBlock(inputPage.getPositionCount());
                    changePvalueBlock = blockFactory.newConstantNullBlock(inputPage.getPositionCount());
                }

                outputPage = inputPage.appendBlocks(new Block[] { changeTypeBlock, changePvalueBlock });
                success = true;
            } finally {
                if (success == false) {
                    Releasables.closeExpectNoException(changeTypeBlock, changePvalueBlock);
                }
            }

            inputPages.removeFirst();
            outputPages.add(outputPage);
            pageStartIndex = pageEndIndex;
        }
    }

    private void emitWarnings(
        boolean tooManyValues,
        boolean hasNulls,
        boolean hasMultivalued,
        boolean hasIndeterminableChangePoint,
        String indeterminableReason
    ) {
        if (tooManyValues) {
            logger.debug(() -> Strings.format("Too many values: limit is %d, some values were ignored", INPUT_VALUE_COUNT_LIMIT));
            warnings(true).registerException(
                new IllegalArgumentException("too many values; keeping only first " + INPUT_VALUE_COUNT_LIMIT + " values")
            );
        }
        if (hasIndeterminableChangePoint) {
            logger.debug(() -> Strings.format("Change point indeterminable: %s", indeterminableReason));
            warnings(false).registerException(new IllegalArgumentException(indeterminableReason));
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

    @Override
    protected void onFinished() {
        createOutputPages();
    }

    @Override
    protected boolean isOperatorFinished() {
        return outputPages.isEmpty();
    }

    @Override
    protected Page onGetOutput() {
        return outputPages.removeFirst();
    }

    @Override
    protected void onClose() {
        Releasables.close(outputPages);
    }

    @Override
    public String toString() {
        return "ChangePointOperator[channel=" + channel + ", groupingChannels=" + Arrays.toString(groupingChannels) + "]";
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
