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
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Find spikes, dips and change point in a list of values.
 * <p>
 * Warning: this operator cannot handle large amounts of data! It buffers all
 * data that is passed to it, runs the change point detector on the data (which
 * is a compute-heavy process), and then outputs all data with the change points.
 */
public class ChangePointOperator implements Operator {

    public static final int INPUT_VALUE_COUNT_LIMIT = 1000;

    public record Factory(int metricChannel, List<Integer> partitionChannel, String sourceText, int sourceLine, int sourceColumn)
        implements
            OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            return new ChangePointOperator(driverContext, metricChannel, partitionChannel, sourceText, sourceLine, sourceColumn);
        }

        @Override
        public String describe() {
            return ChangePointOperator.describe(metricChannel, partitionChannel);
        }
    }

    private final DriverContext driverContext;
    private final int metricChannel;
    private final List<Integer> partitionChannel;
    private final String sourceText;
    private final int sourceLine;
    private final int sourceColumn;

    private final Deque<Page> inputPages;
    private final Deque<Page> outputPages;
    private boolean finished;
    private Warnings warnings;

    // TODO: make org.elasticsearch.xpack.esql.core.tree.Source available here
    // (by modularizing esql-core) and use that instead of the individual fields.
    public ChangePointOperator(
        DriverContext driverContext,
        int metricChannel,
        List<Integer> partitionChannel,
        String sourceText,
        int sourceLine,
        int sourceColumn
    ) {
        this.driverContext = driverContext;
        this.metricChannel = metricChannel;
        this.partitionChannel = partitionChannel;
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
        int maxValuesCount = 0;
        {
            int valuesCount = 0;
            String lastPartitionFieldValue = null;
            for (Page inputPage : inputPages) {
                String currentPartitionFieldValue = getPartitionKey(inputPage, 0);
                if (lastPartitionFieldValue != null) {
                    if (Objects.equals(currentPartitionFieldValue, lastPartitionFieldValue) == false) {
                        valuesCount = 0;
                    }
                }
                lastPartitionFieldValue = currentPartitionFieldValue;
                valuesCount += inputPage.getPositionCount();
                maxValuesCount = Math.max(maxValuesCount, valuesCount);
            }
        }
        boolean tooManyValues = maxValuesCount > INPUT_VALUE_COUNT_LIMIT;

        List<MlAggsHelper.DoubleBucketValues> bucketValuesPerPartition = new ArrayList<>();
        boolean hasNulls = false;
        boolean hasMultivalued = false;
        {
            List<Double> values = new ArrayList<>(maxValuesCount);
            List<Integer> bucketIndexes = new ArrayList<>(maxValuesCount);
            int valuesIndex = 0;
            String lastPartitionFieldValue = null;
            for (Page inputPage : inputPages) {
                String currentPartitionFieldValue = getPartitionKey(inputPage, 0);
                if (lastPartitionFieldValue != null) {
                    if (Objects.equals(currentPartitionFieldValue, lastPartitionFieldValue) == false) {
                        MlAggsHelper.DoubleBucketValues bucketValues = new MlAggsHelper.DoubleBucketValues(
                            null,
                            values.stream().mapToDouble(Double::doubleValue).toArray(),
                            bucketIndexes.stream().mapToInt(Integer::intValue).toArray()
                        );
                        bucketValuesPerPartition.add(bucketValues);

                        values = new ArrayList<>(maxValuesCount);
                        bucketIndexes = new ArrayList<>(maxValuesCount);
                        valuesIndex = 0;
                    }
                }
                lastPartitionFieldValue = currentPartitionFieldValue;
                Block inputBlock = inputPage.getBlock(metricChannel);
                for (int i = 0; i < inputBlock.getPositionCount() && valuesIndex < maxValuesCount; i++, valuesIndex++) {
                    Object value = BlockUtils.toJavaObject(inputBlock, i);
                    if (value == null) {
                        hasNulls = true;
                    } else if (value instanceof List<?>) {
                        hasMultivalued = true;
                    } else {
                        values.add(((Number) value).doubleValue());
                        bucketIndexes.add(valuesIndex);
                    }
                }
            }
            // Handle last partition separately
            // if (lastPartitionFieldValue != null) {
            MlAggsHelper.DoubleBucketValues bucketValues = new MlAggsHelper.DoubleBucketValues(
                null,
                values.stream().mapToDouble(Double::doubleValue).toArray(),
                bucketIndexes.stream().mapToInt(Integer::intValue).toArray()
            );
            bucketValuesPerPartition.add(bucketValues);
            // }
        }

        List<ChangeType> changeTypes = new ArrayList<>();
        {
            for (MlAggsHelper.DoubleBucketValues bucketValues : bucketValuesPerPartition) {
                ChangeType changeType = ChangePointDetector.getChangeType(bucketValues);
                if (changeType instanceof ChangeType.Indeterminable indeterminable) {
                    warnings(false).registerException(new IllegalArgumentException(indeterminable.getReason()));
                }
                changeTypes.add(changeType);
            }
        }

        insertChangePoints(changeTypes);

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

    private void insertChangePoints(Iterable<ChangeType> changeTypes) {
        Iterator<ChangeType> changeTypesIterator = changeTypes.iterator();
        ChangeType changeType = null;
        if (changeTypesIterator.hasNext()) {
            changeType = changeTypesIterator.next();
        }
        BlockFactory blockFactory = driverContext.blockFactory();
        int pageStartIndex = 0;
        String lastPartitionFieldValue = null;
        while (inputPages.isEmpty() == false) {
            Page inputPage = inputPages.peek();
            Page outputPage;
            Block changeTypeBlock = null;
            Block changePvalueBlock = null;
            boolean success = false;

            String currentPartitionFieldValue = getPartitionKey(inputPage, 0);
            if (lastPartitionFieldValue != null) {
                if (Objects.equals(currentPartitionFieldValue, lastPartitionFieldValue) == false) {
                    pageStartIndex = 0;
                    if (changeTypesIterator.hasNext()) {
                        changeType = changeTypesIterator.next();
                    }
                }
            }
            lastPartitionFieldValue = currentPartitionFieldValue;

            try {
                // TODO: How to handle case when there are no change points
                if (changeType != null
                    && pageStartIndex <= changeType.changePoint()
                    && changeType.changePoint() < pageStartIndex + inputPage.getPositionCount()) {
                    try (
                        BytesRefBlock.Builder changeTypeBlockBuilder = blockFactory.newBytesRefBlockBuilder(inputPage.getPositionCount());
                        DoubleBlock.Builder pvalueBlockBuilder = blockFactory.newDoubleBlockBuilder(inputPage.getPositionCount())
                    ) {
                        for (int i = 0; i < inputPage.getPositionCount(); i++) {
                            if (pageStartIndex + i == changeType.changePoint()) {
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
                if (pageStartIndex + inputPage.getPositionCount() > INPUT_VALUE_COUNT_LIMIT) {
                    outputPage = outputPage.subPage(0, INPUT_VALUE_COUNT_LIMIT - pageStartIndex);
                }
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
    }

    /**
     * Calculates the partition key of the i-th row of the given page.
     *
     * @param page page for which the partition key should be calculated
     * @param i row index
     * @return partition key of the i-th row of the given page
     */
    private String getPartitionKey(Page page, int i) {
        if (partitionChannel.isEmpty()) {
            return "";
        }
        assert page.getPositionCount() > 0;
        StringBuilder builder = new StringBuilder();
        for (Integer partitionChannel : partitionChannel) {
            try (var block = page.getBlock(partitionChannel).filter(i)) {
                BytesRef partitionFieldValue = ((BytesRefBlock) block).getBytesRef(i, new BytesRef());
                builder.append(partitionFieldValue.utf8ToString());
            }
        }
        return builder.toString();
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
        return describe(metricChannel, partitionChannel);
    }

    private static String describe(int metricChannel, List<Integer> partitionChannel) {
        return "ChangePointOperator[metricChannel="
            + metricChannel
            + ", partitionChannels="
            + partitionChannel.stream().map(c -> c.toString()).collect(Collectors.joining(",", "[", "]"))
            + "]";
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
