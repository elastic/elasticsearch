/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.grouping;

import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;

import java.math.BigDecimal;
import java.math.RoundingMode;

record NumericEmptyBucketGenerator(double from, double to, double roundTo) implements BlockHash.EmptyBucketGenerator {

    NumericEmptyBucketGenerator(Expression buckets, Expression from, Expression to, FoldContext foldContext) {
        this(
            ((Number) from.fold(foldContext)).doubleValue(),
            ((Number) to.fold(foldContext)).doubleValue(),
            determineRounding(buckets, from, to, foldContext)
        );
    }

    @Override
    public int getEmptyBucketCount() {
        int i = 0;
        for (double bucket = round(Math.floor(from / roundTo) * roundTo, 2); bucket < to; bucket = round(bucket + roundTo, 2)) {
            i++;
        }
        return i;
    }

    @Override
    public Block generate(BlockFactory blockFactory, int maxPositionsInBucket) {
        try (
            DoubleBlock.Builder newBlockBuilder =
                (DoubleBlock.Builder) ElementType.DOUBLE.newBlockBuilder(maxPositionsInBucket, blockFactory)
        ) {
            int i = 0;
            for (double bucket = round(Math.floor(from / roundTo) * roundTo, 2); bucket < to; bucket = round(bucket + roundTo, 2)) {
                newBlockBuilder.appendDouble(bucket);
                i++;
            }
            while (i < maxPositionsInBucket) {
                newBlockBuilder.appendNull();
                i++;
            }
            return newBlockBuilder.build();
        }
    }

    private static double round(double value, int n) {
        return new BigDecimal(value).setScale(n, RoundingMode.HALF_UP).doubleValue();
    }

    static double determineRounding(Expression buckets, Expression from, Expression to, FoldContext foldContext) {
        if (from != null) {
            int b = ((Number) buckets.fold(foldContext)).intValue();
            double f = ((Number) from.fold(foldContext)).doubleValue();
            double t = ((Number) to.fold(foldContext)).doubleValue();
            return pickRounding(b, f, t);
        } else {
            return ((Number) buckets.fold(foldContext)).doubleValue();
        }
    }

    private static double pickRounding(int buckets, double from, double to) {
        double precise = (to - from) / buckets;
        double nextPowerOfTen = Math.pow(10, Math.ceil(Math.log10(precise)));
        double halfPower = nextPowerOfTen / 2;
        return precise < halfPower ? halfPower : nextPowerOfTen;
    }
}
