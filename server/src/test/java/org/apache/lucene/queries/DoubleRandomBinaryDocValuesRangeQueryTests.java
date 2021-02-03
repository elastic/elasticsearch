/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.apache.lucene.queries;

import org.elasticsearch.index.mapper.RangeType;

public class DoubleRandomBinaryDocValuesRangeQueryTests extends BaseRandomBinaryDocValuesRangeQueryTestCase {

    @Override
    protected String fieldName() {
        return "double_range_dv_field";
    }

    @Override
    protected RangeType rangeType() {
        return RangeType.DOUBLE;
    }

    @Override
    protected Range nextRange(int dimensions) throws Exception {
        double value1 = nextDoubleInternal();
        double value2 = nextDoubleInternal();
        double min = Math.min(value1, value2);
        double max = Math.max(value1, value2);
        return new DoubleTestRange(min, max);
    }

    private double nextDoubleInternal() {
        switch (random().nextInt(5)) {
            case 0:
                return Double.NEGATIVE_INFINITY;
            case 1:
                return Double.POSITIVE_INFINITY;
            default:
                if (random().nextBoolean()) {
                    return random().nextDouble();
                } else {
                    return (random().nextInt(15) - 7) / 3d;
                }
        }
    }

    private static class DoubleTestRange extends AbstractRange<Double> {
        double min;
        double max;

        DoubleTestRange(double min, double max) {
            this.min = min;
            this.max = max;
        }

        @Override
        public Double getMin() {
            return min;
        }

        @Override
        protected void setMin(int dim, Object val) {
            assert dim == 0;
            double v = (Double) val;
            if (min < v) {
                max = v;
            } else {
                min = v;
            }
        }

        @Override
        public Double getMax() {
            return max;
        }

        @Override
        protected void setMax(int dim, Object val) {
            assert dim == 0;
            double v = (Double) val;
            if (max > v) {
                min = v;
            } else {
                max = v;
            }
        }

        @Override
        protected boolean isDisjoint(Range o) {
            DoubleTestRange other = (DoubleTestRange)o;
            return this.min > other.max || this.max < other.min;
        }

        @Override
        protected boolean isWithin(Range o) {
            DoubleTestRange other = (DoubleTestRange)o;
            if ((this.min >= other.min && this.max <= other.max) == false) {
                // not within:
                return false;
            }
            return true;
        }

        @Override
        protected boolean contains(Range o) {
            DoubleTestRange other = (DoubleTestRange) o;
            if ((this.min <= other.min && this.max >= other.max) == false) {
                // not contains:
                return false;
            }
            return true;
        }

    }

}
