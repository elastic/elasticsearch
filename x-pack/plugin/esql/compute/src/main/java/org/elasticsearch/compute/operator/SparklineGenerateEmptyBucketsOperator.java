/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.sort.LongDoubleBucketedSort;
import org.elasticsearch.compute.data.sort.LongFloatBucketedSort;
import org.elasticsearch.compute.data.sort.LongIntBucketedSort;
import org.elasticsearch.compute.data.sort.LongLongBucketedSort;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.sort.SortOrder;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * Fills in zero-valued buckets for data produced by the last sparkline aggregation
 * phase and stops the {@code @timestamp} columns. Let's take an example:
 * {@snippet lang="esql" :
 * | STATS mi=SPARKLINE(   MAX(int), @timestamp, 3, "2025-01-01", "2025-01-03"),
 *         md=SPARKLINE(MIN(double), @timestamp, 3, "2025-01-01", "2025-01-03")
 *      BY hostname
 * }
 * <p>
 *     That's going to produce output like:
 * </p>
 * {@snippet lang="txt" :
 * ┌───────────────┬──────────────────┬──────────┐
 * │ MIN(int)      │ MAX(double)      │ hostname │
 * ├───────────────┼──────────────────┼──────────┤
 * │ [10, 20, 30]  │ [1.0, 2.0, 3.0]  │ yay      │
 * │ [42,  0, 99]  │ [4.2, 0.0, 9.9]  │ server   │
 * └───────────────┴──────────────────┴──────────┘
 * }
 * <p>
 *     To get there the original ESQL is rewritten into:
 * </p>
 * {@snippet lang="esql" :
 * | STATS mi=MAX(int), md=MIN(double), BY @timestamp=DATE_TRUNC(1 day, @timestamp), hostname
 * | STATS TOP(@timestamp, 3, "asc", mi), TOP(@timestamp, 3, "asc", md), TOP(@timestamp, 3, "asc")
        BY hostname
 * | SPARKLINE_GENERATE_EMPTY_BUCKETS <-- you are here
 * }
 * <p>
 *     The first {@code STATS} is just pretty normal agg. Imagine it returns something like:
 * </p>
 * {@snippet lang="txt" :
 * ┌───────────────┬──────────────────┬────────────┬──────────┐
 * │ MIN(int)      │ MAX(double)      │ @timestamp │ hostname │
 * ├───────────────┼──────────────────┼────────────┼──────────┤
 * │ 10            │ 1.0              │ 2025-01-01 │ yay      │
 * │ 20            │ 2.0              │ 2025-01-02 │ yay      │
 * │ 30            │ 3.0              │ 2025-01-03 │ yay      │
 * │ 42            │ 4.2              │ 2025-01-01 │ server   │
 * │ 99            │ 9.9              │ 2025-01-03 │ server   │
 * └───────────────┴──────────────────┴────────────┴──────────┘
 * }
 * <p>
 *     The second {@code STATS} collects the results into the arrays that the output
 *     layout expects. But have a look:
 * </p>
 * {@snippet lang="txt" :
 * ┌───────────────┬──────────────────┬──────────────────────────────────────┬──────────┐
 * │ MIN(int)      │ MAX(double)      │ @timestamp                           │ hostname │
 * ├───────────────┼──────────────────┼──────────────────────────────────────┼──────────┤
 * │ [10, 20, 30]  │ [1.0, 2.0, 3.0]  │ [2025-01-01, 2025-01-02, 2025-01-03] │ yay      │
 * │ [42, 99]      │ [4.2, 9.9]       │ [2025-01-01, 2025-01-03]             │ server   │
 * └───────────────┴──────────────────┴──────────────────────────────────────┴──────────┘
 * }
 * <p>
 *     See?! The second row is complete - ready! It's what we want! Well, it has a
 *     {@code @timestamp} we don't want, but it's close! But the second row is trouble.
 *     It's missing the value for {@code 2025-01-02}. You can tell which day is missing
 *     by looking at the timestamp. This operator fills in the 0. It'll consume the
 *     data above and make:
 * </p>
 * {@snippet lang="txt" :
 * ┌───────────────┬──────────────────┬──────────┐
 * │ MIN(int)      │ MAX(double)      │ hostname │
 * ├───────────────┼──────────────────┼──────────┤
 * │ [10, 20, 30]  │ [1.0, 2.0, 3.0]  │ yay      │
 * │ [42,  0, 99]  │ [4.2, 0.0, 9.9]  │ server   │
 * └───────────────┴──────────────────┴──────────┘
 * }
 * <p>
 *     And <strong>that</strong> is exactly what we want.
 * </p>
 */
public class SparklineGenerateEmptyBucketsOperator implements Operator {
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
    private final Deque<Page> outputPages;
    private final int numValueColumns;
    private final List<Long> dateBuckets;

    public SparklineGenerateEmptyBucketsOperator(
        DriverContext driverContext,
        int numValueColumns,
        Rounding.Prepared dateBucketRounding,
        long minDate,
        long maxDate
    ) {
        this.driverContext = driverContext;
        this.finished = false;
        outputPages = new ArrayDeque<>();
        this.numValueColumns = numValueColumns;
        this.dateBuckets = calculateDateBuckets(dateBucketRounding, minDate, maxDate);
    }

    @Override
    public boolean needsInput() {
        return finished == false;
    }

    @Override
    public void addInput(Page page) {
        try {
            createOutputPage(page);
        } finally {
            page.releaseBlocks();
        }
    }

    @Override
    public void finish() {
        if (finished == false) {
            finished = true;
        }
    }

    @Override
    public boolean isFinished() {
        return finished && outputPages.isEmpty();
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        return false;
    }

    @Override
    public Page getOutput() {
        // TODO this could probably stream buckets rather than accumulate
        if (finished == false || outputPages.isEmpty()) {
            return null;
        }
        return outputPages.removeFirst();
    }

    @Override
    public void close() {
        for (Page page : outputPages) {
            page.releaseBlocks();
        }
    }

    @Override
    public String toString() {
        return "SparklineGenerateEmptyBucketsOperator[numValueColumns=" + numValueColumns + "]";
    }

    private void createOutputPage(Page inputPage) {
        LongBlock dateBlock = inputPage.getBlock(numValueColumns);
        int positionCount = inputPage.getBlock(0).getPositionCount();

        Block[] outputValueBlocks = new Block[numValueColumns];
        try {
            for (int v = 0; v < numValueColumns; v++) {
                Block valueBlock = inputPage.getBlock(v);
                try (
                    // TODO we could probably build the output on the fly
                    OutputBucketedSort outputBucketedSort = new OutputBucketedSort(
                        valueBlock,
                        driverContext.bigArrays(),
                        dateBuckets.size()
                    )
                ) {
                    for (int groupId = 0; groupId < positionCount; groupId++) {
                        int startGroupIndex = valueBlock.getFirstValueIndex(groupId);
                        int endGroupIndex = startGroupIndex + valueBlock.getValueCount(groupId);
                        int currentIndex = startGroupIndex;
                        for (long dateBucket : dateBuckets) {
                            if (dateBlock.areAllValuesNull() == false
                                && currentIndex < endGroupIndex
                                && dateBucket == dateBlock.getLong(currentIndex)) {
                                outputBucketedSort.collectValueBlockValueAtIndex(dateBucket, groupId, currentIndex);
                                currentIndex++;
                            } else {
                                outputBucketedSort.collectDefaultValue(dateBucket, groupId);
                            }
                        }
                    }

                    Block[] sortBlocks = new Block[2];
                    try (IntVector groupIds = driverContext.blockFactory().newIntRangeVector(0, positionCount)) {
                        outputBucketedSort.toBlocks(driverContext.blockFactory(), sortBlocks, groupIds);
                    }
                    sortBlocks[0].close();
                    outputValueBlocks[v] = sortBlocks[1];
                }
            }
        } catch (Exception e) {
            Releasables.closeExpectNoException(outputValueBlocks);
            throw e;
        }

        // TODO appendBlocks?
        Page outputPage = new Page(outputValueBlocks);
        int passthroughStart = numValueColumns + 1;
        for (int i = passthroughStart; i < inputPage.getBlockCount(); i++) {
            Block passthroughBlock = inputPage.getBlock(i);
            passthroughBlock.incRef();
            outputPage = outputPage.appendBlock(passthroughBlock);
        }
        outputPages.add(outputPage);
    }

    private List<Long> calculateDateBuckets(Rounding.Prepared dateBucketRounding, long minDate, long maxDate) {
        List<Long> dateBuckets = new ArrayList<>();
        long currentDateBucket = dateBucketRounding.round(minDate);
        while (currentDateBucket <= maxDate) {
            dateBuckets.add(currentDateBucket);
            currentDateBucket = dateBucketRounding.nextRoundingValue(currentDateBucket);
        }
        return dateBuckets;
    }

    private static class OutputBucketedSort implements Releasable {
        // TODO replace BucketedSort with a little hand built copy-and-expand
        // TODO if the input block is *perfect* just use it
        private final Block valueBlock;
        private final Releasable bucketedSort;

        OutputBucketedSort(Block valueBlock, BigArrays bigArrays, int valueCountLimit) {
            this.valueBlock = valueBlock;
            switch (valueBlock.elementType()) {
                case LONG -> bucketedSort = new LongLongBucketedSort(bigArrays, SortOrder.ASC, valueCountLimit);
                case INT -> bucketedSort = new LongIntBucketedSort(bigArrays, SortOrder.ASC, valueCountLimit);
                case DOUBLE -> bucketedSort = new LongDoubleBucketedSort(bigArrays, SortOrder.ASC, valueCountLimit);
                case FLOAT -> bucketedSort = new LongFloatBucketedSort(bigArrays, SortOrder.ASC, valueCountLimit);
                case NULL -> bucketedSort = new LongLongBucketedSort(bigArrays, SortOrder.ASC, valueCountLimit);
                default -> throw new IllegalArgumentException("Unsupported element type [" + valueBlock.elementType() + "]");
            }
        }

        public void collectDefaultValue(long dateBucket, int groupId) {
            switch (valueBlock.elementType()) {
                case LONG -> ((LongLongBucketedSort) bucketedSort).collect(dateBucket, 0L, groupId);
                case INT -> ((LongIntBucketedSort) bucketedSort).collect(dateBucket, 0, groupId);
                case DOUBLE -> ((LongDoubleBucketedSort) bucketedSort).collect(dateBucket, 0d, groupId);
                case FLOAT -> ((LongFloatBucketedSort) bucketedSort).collect(dateBucket, 0f, groupId);
                case NULL -> ((LongLongBucketedSort) bucketedSort).collect(dateBucket, 0L, groupId);
                default -> throw new IllegalArgumentException("Unsupported element type [" + valueBlock.elementType() + "]");
            }
        }

        public void collectValueBlockValueAtIndex(long dateBucket, int groupId, int valueIndex) {
            switch (valueBlock.elementType()) {
                case LONG -> ((LongLongBucketedSort) bucketedSort).collect(
                    dateBucket,
                    ((LongBlock) valueBlock).getLong(valueIndex),
                    groupId
                );
                case INT -> ((LongIntBucketedSort) bucketedSort).collect(dateBucket, ((IntBlock) valueBlock).getInt(valueIndex), groupId);
                case DOUBLE -> ((LongDoubleBucketedSort) bucketedSort).collect(
                    dateBucket,
                    ((DoubleBlock) valueBlock).getDouble(valueIndex),
                    groupId
                );
                case FLOAT -> ((LongFloatBucketedSort) bucketedSort).collect(
                    dateBucket,
                    ((FloatBlock) valueBlock).getFloat(valueIndex),
                    groupId
                );
                case NULL -> ((LongLongBucketedSort) bucketedSort).collect(dateBucket, 0L, groupId);
                default -> throw new IllegalArgumentException("Unsupported element type [" + valueBlock.elementType() + "]");
            }
        }

        private void toBlocks(BlockFactory blockFactory, Block[] blocks, IntVector groupIds) {
            switch (valueBlock.elementType()) {
                case LONG -> ((LongLongBucketedSort) bucketedSort).toBlocks(blockFactory, blocks, 0, groupIds);
                case INT -> ((LongIntBucketedSort) bucketedSort).toBlocks(blockFactory, blocks, 0, groupIds);
                case DOUBLE -> ((LongDoubleBucketedSort) bucketedSort).toBlocks(blockFactory, blocks, 0, groupIds);
                case FLOAT -> ((LongFloatBucketedSort) bucketedSort).toBlocks(blockFactory, blocks, 0, groupIds);
                case NULL -> ((LongLongBucketedSort) bucketedSort).toBlocks(blockFactory, blocks, 0, groupIds);
                default -> throw new IllegalArgumentException("Unsupported element type [" + valueBlock.elementType() + "]");
            }
        }

        @Override
        public void close() {
            bucketedSort.close();
        }
    }
}
