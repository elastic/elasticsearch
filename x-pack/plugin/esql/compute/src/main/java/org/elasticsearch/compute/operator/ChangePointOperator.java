/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.ml.aggs.MlAggsHelper;
import org.elasticsearch.xpack.ml.aggs.changepoint.ChangePointDetector;
import org.elasticsearch.xpack.ml.aggs.changepoint.ChangeType;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * Find spikes, dips and change point in a list of values.
 * <p>
 * Warning: this operator cannot handle large amounts of data! It buffers all
 * data that is passed to it, runs the change point detector on the data (which
 * is a compute-heavy process), and then outputs all data with the change points.
 */
public class ChangePointOperator implements Operator {

    public static final int INPUT_VALUE_COUNT_LIMIT = 1000;

    public record Factory(int channel, String sourceText, int sourceLine, int sourceColumn) implements OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            return new ChangePointOperator(driverContext, channel, sourceText, sourceLine, sourceColumn);
        }

        @Override
        public String describe() {
            return "ChangePointOperator[channel=" + channel + "]";
        }
    }

    private final DriverContext driverContext;
    private final int channel;
    private final String sourceText;
    private final int sourceLine;
    private final int sourceColumn;

    private final Deque<Page> inputPages;
    private final Deque<Page> outputPages;
    private boolean finished;
    private Warnings warnings;

    // TODO: make org.elasticsearch.xpack.esql.core.tree.Source available here
    // (by modularizing esql-core) and use that instead of the individual fields.
    public ChangePointOperator(DriverContext driverContext, int channel, String sourceText, int sourceLine, int sourceColumn) {
        this.driverContext = driverContext;
        this.channel = channel;
        this.sourceText = sourceText;
        this.sourceLine = sourceLine;
        this.sourceColumn = sourceColumn;

        finished = false;
        inputPages = new ArrayDeque<>();
        outputPages = new ArrayDeque<>();
        warnings = null;
    }

    @Override
    public boolean needsInput() {
        return finished == false;
    }

    @Override
    public void addInput(Page page) {
        inputPages.add(page);
    }

    @Override
    public void finish() {
        if (finished == false) {
            finished = true;
            createOutputPages();
        }
    }

    @Override
    public boolean isFinished() {
        return finished && outputPages.isEmpty();
    }

    @Override
    public Page getOutput() {
        if (finished == false || outputPages.isEmpty()) {
            return null;
        }
        return outputPages.removeFirst();
    }

    private void createOutputPages() {
        int valuesCount = 0;
        for (Page page : inputPages) {
            valuesCount += page.getPositionCount();
        }
        boolean tooManyValues = valuesCount > INPUT_VALUE_COUNT_LIMIT;
        if (tooManyValues) {
            valuesCount = INPUT_VALUE_COUNT_LIMIT;
        }

        List<Double> values = new ArrayList<>(valuesCount);
        List<Integer> bucketIndexes = new ArrayList<>(valuesCount);
        int valuesIndex = 0;
        boolean hasNulls = false;
        boolean hasMultivalued = false;
        for (Page inputPage : inputPages) {
            Block inputBlock = inputPage.getBlock(channel);
            for (int i = 0; i < inputBlock.getPositionCount() && valuesIndex < valuesCount; i++) {
                Object value = BlockUtils.toJavaObject(inputBlock, i);
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

        MlAggsHelper.DoubleBucketValues bucketValues = new MlAggsHelper.DoubleBucketValues(
            null,
            values.stream().mapToDouble(Double::doubleValue).toArray(),
            bucketIndexes.stream().mapToInt(Integer::intValue).toArray()
        );
        ChangeType changeType = ChangePointDetector.getChangeType(bucketValues);
        int changePointIndex = changeType.changePoint();

        BlockFactory blockFactory = driverContext.blockFactory();
        int pageStartIndex = 0;
        while (inputPages.isEmpty() == false) {
            Page inputPage = inputPages.peek();
            Page outputPage;
            Block changeTypeBlock = null;
            Block changePvalueBlock = null;
            boolean success = false;
            try {
                if (pageStartIndex <= changePointIndex && changePointIndex < pageStartIndex + inputPage.getPositionCount()) {
                    try (
                        BytesRefBlock.Builder changeTypeBlockBuilder = blockFactory.newBytesRefBlockBuilder(inputPage.getPositionCount());
                        DoubleBlock.Builder pvalueBlockBuilder = blockFactory.newDoubleBlockBuilder(inputPage.getPositionCount())
                    ) {
                        for (int i = 0; i < inputPage.getPositionCount(); i++) {
                            if (pageStartIndex + i == changePointIndex) {
                                changeTypeBlockBuilder.appendBytesRef(new BytesRef(changeType.getWriteableName()));
                                pvalueBlockBuilder.appendDouble(changeType.pValue());
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
            pageStartIndex += inputPage.getPositionCount();
        }

        if (changeType instanceof ChangeType.Indeterminable indeterminable) {
            warnings(false).registerException(new IllegalArgumentException(indeterminable.getReason()));
        }
        if (tooManyValues) {
            warnings(true).registerException(
                new IllegalArgumentException("too many values; keeping only first " + INPUT_VALUE_COUNT_LIMIT + " values")
            );
        }
        if (hasNulls) {
            warnings(true).registerException(new IllegalArgumentException("values contain nulls; skipping them"));
        }
        if (hasMultivalued) {
            warnings(true).registerException(
                new IllegalArgumentException(
                    "values contains multivalued entries; skipping them (please consider reducing them with e.g. MV_AVG or MV_SUM)"
                )
            );
        }
    }

    @Override
    public void close() {
        for (Page page : inputPages) {
            page.releaseBlocks();
        }
        for (Page page : outputPages) {
            page.releaseBlocks();
        }
    }

    @Override
    public String toString() {
        return "ChangePointOperator[channel=" + channel + "]";
    }

    private Warnings warnings(boolean onlyWarnings) {
        if (warnings == null) {
            if (onlyWarnings) {
                this.warnings = Warnings.createOnlyWarnings(driverContext.warningsMode(), sourceLine, sourceColumn, sourceText);
            } else {
                this.warnings = Warnings.createWarnings(driverContext.warningsMode(), sourceLine, sourceColumn, sourceText);
            }
        }
        return warnings;
    }
}
