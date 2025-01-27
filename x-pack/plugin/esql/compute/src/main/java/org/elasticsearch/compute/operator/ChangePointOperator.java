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
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.ml.aggs.MlAggsHelper;
import org.elasticsearch.xpack.ml.aggs.changepoint.ChangePointDetector;
import org.elasticsearch.xpack.ml.aggs.changepoint.ChangeType;

import java.util.ArrayList;
import java.util.List;

public class ChangePointOperator implements Operator {
    private static final Logger logger = LogManager.getLogger(ChangePointOperator.class);

    public record Factory(int inputChannel) implements OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            return new ChangePointOperator(driverContext.blockFactory(), inputChannel);
        }

        @Override
        public String describe() {
            return "ChangePointOperator[input=" + inputChannel + "]";
        }
    }

    private final BlockFactory blockFactory;
    private final int inputChannel;
    private boolean finished;
    private List<Page> inputPages;
    private List<Page> outputPages;
    private int outputPageIndex;

    public ChangePointOperator(BlockFactory blockFactory, int inputChannel) {
        this.blockFactory = blockFactory;
        this.inputChannel = inputChannel;

        finished = false;
        inputPages = new ArrayList<>();
        outputPages = new ArrayList<>();
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
        finished = true;
        createOutputPages();
    }

    @Override
    public boolean isFinished() {
        return finished && outputPageIndex == inputPages.size();
    }

    @Override
    public Page getOutput() {
        if (finished == false) {
            return null;
        }
        if (outputPageIndex == inputPages.size()) {
            return null;
        }
        return outputPages.get(outputPageIndex++);
    }

    private void createOutputPages() {
        int valuesCount = 0;
        for (Page page : inputPages) {
            valuesCount += page.getPositionCount();
        }

        double[] values = new double[valuesCount];
        int valuesIndex = 0;
        for (Page inputPage : inputPages) {
            // TODO: other data types
            LongVector vector = (LongVector) inputPage.getBlock(inputChannel).asVector();
            for (int i = 0; i < vector.getPositionCount(); i++) {
                values[valuesIndex++] = (double) vector.getLong(i);
            }
        }
        logger.warn("***ALL DATA*** (#pages={}) {}", inputPages.size(), values);

        ChangeType changeType =
            ChangePointDetector.getChangeType(new MlAggsHelper.DoubleBucketValues(null, values));
        int changePointIndex = changeType.changePoint();

        int pageStartIndex = 0;
        for (Page inputPage : inputPages) {
            Block changeTypeBlock;
            Block changePvalueBlock;

            if (pageStartIndex <= changePointIndex && changePointIndex < pageStartIndex + inputPage.getPositionCount()) {
                BytesRefBlock.Builder changeTypeBlockBuilder = blockFactory.newBytesRefBlockBuilder(inputPage.getPositionCount());
                DoubleBlock.Builder pvalueBlockBuilder = blockFactory.newDoubleBlockBuilder(inputPage.getPositionCount());
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
            } else {
                changeTypeBlock = blockFactory.newConstantNullBlock(inputPage.getPositionCount());
                changePvalueBlock = blockFactory.newConstantNullBlock(inputPage.getPositionCount());
            }

            // TODO: make sure it's the right channel
            Page outputPage = inputPage.appendBlocks(new Block[] { changeTypeBlock, changePvalueBlock });
            outputPages.add(outputPage);

            pageStartIndex += inputPage.getPositionCount();
        }
    }

    @Override
    public void close() {

    }
}
