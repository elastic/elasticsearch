/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.exponentialhistogram.ExponentialHistogram;

public sealed interface ExponentialHistogramBlock extends Block permits ConstantNullBlock, ExponentialHistogramArrayBlock {

    ExponentialHistogram getExponentialHistogram(int valueIndex);

    static boolean equals(ExponentialHistogramBlock blockA, ExponentialHistogramBlock blockB) {
        if (blockA == blockB) {
            return true;
        }
        return switch (blockA) {
            case null -> false;
            case ConstantNullBlock a -> a.equals(blockB);
            case ExponentialHistogramArrayBlock a -> switch (blockB) {
                case null -> false;
                case ConstantNullBlock b -> b.equals(a);
                case ExponentialHistogramArrayBlock b -> a.equalsAfterTypeCheck(b);
            };
        };
    }

}
