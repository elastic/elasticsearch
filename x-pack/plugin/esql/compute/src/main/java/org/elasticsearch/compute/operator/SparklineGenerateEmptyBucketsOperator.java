/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.sort.LongLongBucketedSort;
import org.elasticsearch.search.sort.SortOrder;

import java.util.ArrayDeque;
import java.util.Deque;

public class SparklineGenerateEmptyBucketsOperator implements Operator {
    public static final int SPARKLINE_VALUE_COUNT_LIMIT = 1000;

    public record Factory(int numValueColumns, Rounding.Prepared dateBucketRounding, long minDate, long maxDate)
        implements
            OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            return new SparklineGenerateEmptyBucketsOperator(driverContext, numValueColumns, dateBucketRounding, minDate, maxDate);
        }

        @Override
        public String describe() {
            return "SparklineGenerateEmptyBucketsOperator[numValueColumns=" + numValueColumns + "]";
        }
    }

    private final DriverContext driverContext;
    private boolean finished;
    private final Deque<Page> inputPages;
    private final Deque<Page> outputPages;
    private final int numValueColumns;
    private final Rounding.Prepared dateBucketRounding;
    private final long minDate;
    private final long maxDate;

    public SparklineGenerateEmptyBucketsOperator(
        DriverContext driverContext,
        int numValueColumns,
        Rounding.Prepared dateBucketRounding,
        long minDate,
        long maxDate
    ) {
        this.driverContext = driverContext;
        this.finished = false;
        inputPages = new ArrayDeque<>();
        outputPages = new ArrayDeque<>();
        this.numValueColumns = numValueColumns;
        this.dateBucketRounding = dateBucketRounding;
        this.minDate = minDate;
        this.maxDate = maxDate;
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
        long minDateBucket = dateBucketRounding.round(minDate);
        long maxDateBucket = dateBucketRounding.round(maxDate);

        for (Page inputPage : inputPages) {
            LongBlock dateBlock = inputPage.getBlock(numValueColumns);
            LongBlock firstValueBlock = inputPage.getBlock(0);
            int positionCount = firstValueBlock.getPositionCount();
            int[] groupIds = new int[positionCount];
            for (int g = 0; g < positionCount; g++) {
                groupIds[g] = g;
            }

            Block[] filledValueBlocks = new Block[numValueColumns];
            for (int v = 0; v < numValueColumns; v++) {
                // TODO: Instead of making arbitrarily large buckets, we should make them as large as the number of buckets.
                LongLongBucketedSort bucketedSort = new LongLongBucketedSort(
                    driverContext.bigArrays(),
                    SortOrder.ASC,
                    SPARKLINE_VALUE_COUNT_LIMIT
                );
                LongBlock valueBlock = inputPage.getBlock(v);

                for (int groupId = 0; groupId < positionCount; groupId++) {
                    int startGroupIndex = valueBlock.getFirstValueIndex(groupId);
                    int endGroupIndex = startGroupIndex + valueBlock.getValueCount(groupId);
                    int currentIndex = startGroupIndex;
                    long currentDateBucket = minDateBucket;
                    while (currentDateBucket <= maxDateBucket) {
                        if (currentIndex < endGroupIndex && currentDateBucket == dateBlock.getLong(currentIndex)) {
                            bucketedSort.collect(currentDateBucket, valueBlock.getLong(currentIndex), groupId);
                            currentIndex++;
                        } else {
                            bucketedSort.collect(currentDateBucket, 0L, groupId);
                        }
                        currentDateBucket = dateBucketRounding.nextRoundingValue(currentDateBucket);
                    }
                }

                Block[] sortBlocks = new Block[2];
                bucketedSort.toBlocks(
                    driverContext.blockFactory(),
                    sortBlocks,
                    0,
                    driverContext.blockFactory().newIntArrayVector(groupIds, positionCount)
                );
                filledValueBlocks[v] = sortBlocks[1];
            }

            Page outputPage = new Page(filledValueBlocks);
            int passthroughStart = numValueColumns + 1;
            for (int i = passthroughStart; i < inputPage.getBlockCount(); i++) {
                outputPage = outputPage.appendBlock(inputPage.getBlock(i));
            }
            outputPages.add(outputPage);
        }
    }

    @Override
    public void close() {}
}
