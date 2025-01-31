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
import org.elasticsearch.xpack.ml.aggs.MlAggsHelper;
import org.elasticsearch.xpack.ml.aggs.changepoint.ChangePointDetector;
import org.elasticsearch.xpack.ml.aggs.changepoint.ChangeType;

import java.util.ArrayList;
import java.util.List;

public class ChangePointOperator implements Operator {

    // TODO: close upon failure / interrupt

    public static final int INPUT_VALUE_COUNT_LIMIT = 1000;

    public record Factory(int inputChannel, String sourceText, int sourceLine, int sourceColumn) implements OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            return new ChangePointOperator(driverContext, inputChannel, sourceText, sourceLine, sourceColumn);
        }

        @Override
        public String describe() {
            return "ChangePointOperator[input=" + inputChannel + "]";
        }
    }

    private final DriverContext driverContext;
    private final int inputChannel;
    private final String sourceText;
    private final int sourceLine;
    private final int sourceColumn;

    private final List<Page> inputPages;
    private final List<Page> outputPages;
    private boolean finished;
    private int outputPageIndex;
    private Warnings warnings;

    public ChangePointOperator(DriverContext driverContext, int inputChannel, String sourceText, int sourceLine, int sourceColumn) {
        this.driverContext = driverContext;
        this.inputChannel = inputChannel;
        this.sourceText = sourceText;
        this.sourceLine = sourceLine;
        this.sourceColumn = sourceColumn;

        finished = false;
        inputPages = new ArrayList<>();
        outputPages = new ArrayList<>();
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
        if (finished == false) {
            return null;
        }
        if (outputPageIndex == outputPages.size()) {
            outputPages.clear();
            return null;
        }
        return outputPages.get(outputPageIndex++);
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

        // TODO: account for this memory?
        double[] values = new double[valuesCount];
        int valuesIndex = 0;
        boolean hasNulls = false;
        for (Page inputPage : inputPages) {
            Block inputBlock = inputPage.getBlock(inputChannel);
            for (int i = 0; i < inputBlock.getPositionCount() && valuesIndex < valuesCount; i++) {
                Object value = BlockUtils.toJavaObject(inputBlock, i);
                if (value == null) {
                    hasNulls = true;
                    values[valuesIndex++] = 0;
                } else {
                    values[valuesIndex++] = ((Number) value).doubleValue();
                }
            }
        }

        ChangeType changeType = ChangePointDetector.getChangeType(new MlAggsHelper.DoubleBucketValues(null, values));
        int changePointIndex = changeType.changePoint();

        BlockFactory blockFactory = driverContext.blockFactory();
        int pageStartIndex = 0;
        for (Page inputPage : inputPages) {
            Block changeTypeBlock;
            Block changePvalueBlock;
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

            Page outputPage = inputPage.appendBlocks(new Block[] { changeTypeBlock, changePvalueBlock });
            outputPages.add(outputPage);
            pageStartIndex += inputPage.getPositionCount();
        }

        inputPages.clear();

        if (changeType instanceof ChangeType.Indeterminable indeterminable) {
            warnings(false).registerException(new IllegalArgumentException(indeterminable.getReason()));
        }
        if (tooManyValues) {
            warnings(true).registerException(
                new IllegalArgumentException("too many values; keeping only first " + INPUT_VALUE_COUNT_LIMIT + " values")
            );
        }
        if (hasNulls) {
            warnings(true).registerException(new IllegalArgumentException("values contain nulls; treating them as zeroes"));
        }
    }

    @Override
    public void close() {}

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
