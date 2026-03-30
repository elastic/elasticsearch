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
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.ml.aggs.MlAggsHelper;
import org.elasticsearch.xpack.ml.aggs.changepoint.ChangePointDetector;
import org.elasticsearch.xpack.ml.aggs.changepoint.ChangeType;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Objects;

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

    public record Factory(int channel, Integer groupingChannel, WarningSourceLocation source) implements OperatorFactory {

        @Override
        public Operator get(DriverContext driverContext) {
            return new ChangePointOperator(driverContext, channel, groupingChannel, source);
        }

        @Override
        public String describe() {
            return groupingChannel == null
                ? Strings.format("ChangePointOperator[channel=%d]", channel)
                : Strings.format("ChangePointOperator[channel=%d, groupingChannel=%d]", channel, groupingChannel);
        }
    }

    private final DriverContext driverContext;
    private final int channel;
    private final Integer groupChannel;
    private final WarningSourceLocation source;

    private final Deque<Page> outputPages;
    private Warnings warnings;

    public ChangePointOperator(DriverContext driverContext, int channel, Integer groupingChannel, WarningSourceLocation source) {
        this.driverContext = driverContext;
        this.channel = channel;
        this.groupChannel = groupingChannel;
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
        int valuesCount = 0;
        for (Page page : inputPages) {
            valuesCount += page.getPositionCount();
        }
        boolean tooManyValues = valuesCount > INPUT_VALUE_COUNT_LIMIT;
        if (tooManyValues) {
            valuesCount = INPUT_VALUE_COUNT_LIMIT; // TODO Should this remain the total limit or limit per group?
        }

        List<Double> values = new ArrayList<>();
        List<Integer> bucketIndexes = new ArrayList<>();
        ArrayDeque<DetectedChangePoint> detectedChangePoints = new ArrayDeque<>();
        int valuesIndex = 0;
        Object previousGroupKey = groupChannel != null
            ? BlockUtils.toJavaObject(inputPages.peek().getBlock(groupChannel), 0) // TODO can we got no input?
            : null;

        boolean hasNulls = false;
        boolean hasMultivalued = false;
        boolean hasIndeterminableChangePoint = false;
        boolean lastGroupHasRows = false;
        String indeterminableChangePointReason = "";
        for (Page inputPage : inputPages) {
            Block inputBlock = inputPage.getBlock(channel);
            Block groupBlock = groupChannel != null ? inputPage.getBlock(groupChannel) : null;
            for (int i = 0; i < inputBlock.getPositionCount() && valuesIndex < valuesCount; i++) {
                if (groupBlock != null) {
                    Object currentGroupKey = BlockUtils.toJavaObject(groupBlock, i);
                    if (Objects.equals(currentGroupKey, previousGroupKey) == false) {
                        if (values.isEmpty() == false) {
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
                        previousGroupKey = currentGroupKey;
                    }
                }

                Object value = BlockUtils.toJavaObject(inputBlock, i);
                lastGroupHasRows = true;
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
        // runs  to produce an "indeterminable" warning.
        if (values.isEmpty() == false || groupChannel == null || lastGroupHasRows) {
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
        boolean tooManyValues, boolean hasNulls, boolean hasMultivalued, boolean hasIndeterminableChangePoint,
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
            logger.debug("Values contain nulls; skipping them");
            warnings(true).registerException(new IllegalArgumentException("values contain nulls; skipping them"));
        }
        if (hasMultivalued) {
            logger.debug("Values contain multivalued entries; skipping them");
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
        if (groupChannel == null) {
            return "ChangePointOperator[channel=" + channel + "]";
        }
        else {
            return "ChangePointOperator[channel=" + channel + ", groupChannel=" + groupChannel + "]";
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
