/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2021 The OpenNMS Group, Inc.
 * OpenNMS(R) is Copyright (C) 1999-2021 The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is a registered trademark of The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * OpenNMS(R) is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with OpenNMS(R).  If not, see:
 *      http://www.gnu.org/licenses/
 *
 * For more information contact:
 *     OpenNMS(R) Licensing <license@opennms.org>
 *     http://www.opennms.org/
 *     http://www.opennms.com/
 *******************************************************************************/

package org.elasticsearch.search.aggregations.bucket.histogram;

/**
 * Captures context information that is available to subaggregations of
 * {@link RangeFieldBucketAggregator} ancestor aggregations.
 *
 * @param <N> The type of range and bucket bounds.
 */
public abstract class RangeFieldBucketAggregatorCollectContext<N extends Number> {

    public final N rangeFrom;
    public final N rangeTo;
    public final N bucketFrom;
    public final N bucketTo;

    public RangeFieldBucketAggregatorCollectContext(N rangeFrom, N rangeTo, N bucketFrom, N bucketTo) {
        this.rangeFrom = rangeFrom;
        this.rangeTo = rangeTo;
        this.bucketFrom = bucketFrom;
        this.bucketTo = bucketTo;
    }

    /**
     * Calculates the proportional share of a value corresponding to the length of the overlap of
     * the bucket with the range in relation to the length of the range.
     */
    public abstract double proportionalValue(double totalValue);

    public static class Lng extends RangeFieldBucketAggregatorCollectContext<Long> {
        public Lng(Long rangeFrom, Long rangeTo, Long bucketFrom, Long bucketTo) {
            super(rangeFrom, rangeTo, bucketFrom, bucketTo);
        }

        @Override
        public double proportionalValue(double totalValue) {
            long rangeLength = rangeTo - rangeFrom;

            long overlapFrom = Math.max(rangeFrom, bucketFrom);
            long overlapTo = Math.min(rangeTo, bucketTo);

            // if the proportionalValue would simply be calculated by
            //
            // proportionalValue = totalValue * (overlapTo - overlapFrom) / rangeLength
            //
            // the rounding error would accumulate. It is better to calculate the proportional
            // value by calculating values at the overlap start and at the overlap end and subtract
            // these values. By doing so, rounding errors cancel out.

            double valueAtFrom = (overlapFrom - rangeFrom) * totalValue / rangeLength;
            double valueAtTo = (overlapTo - rangeFrom) * totalValue / rangeLength;
            return valueAtTo - valueAtFrom;
        }
    }

    public static class Dbl extends RangeFieldBucketAggregatorCollectContext<Double> {
        public Dbl(Double rangeFrom, Double rangeTo, Double bucketFrom, Double bucketTo) {
            super(rangeFrom, rangeTo, bucketFrom, bucketTo);
        }

        @Override
        public double proportionalValue(double totalValue) {
            double rangeLength = rangeTo - rangeFrom;

            double overlapFrom = Math.max(rangeFrom, bucketFrom);
            double overlapTo = Math.min(rangeTo, bucketTo);

            double valueAtFrom = (overlapFrom - rangeFrom) * totalValue / rangeLength;
            double valueAtTo = (overlapTo - rangeFrom) * totalValue / rangeLength;

            return valueAtTo - valueAtFrom;
        }
    }

}
