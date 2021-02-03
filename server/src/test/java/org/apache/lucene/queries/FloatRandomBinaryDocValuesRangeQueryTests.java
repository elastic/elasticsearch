/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.apache.lucene.queries;

import org.elasticsearch.index.mapper.RangeType;

public class FloatRandomBinaryDocValuesRangeQueryTests extends BaseRandomBinaryDocValuesRangeQueryTestCase {

    @Override
    protected String fieldName() {
        return "float_range_dv_field";
    }

    @Override
    protected RangeType rangeType() {
        return RangeType.FLOAT;
    }

    @Override
    protected Range nextRange(int dimensions) throws Exception {
        float value1 = nextFloatInternal();
        float value2 = nextFloatInternal();
        float min = Math.min(value1, value2);
        float max = Math.max(value1, value2);
        return new FloatTestRange(min, max);
    }

    private float nextFloatInternal() {
        switch (random().nextInt(5)) {
            case 0:
                return Float.NEGATIVE_INFINITY;
            case 1:
                return Float.POSITIVE_INFINITY;
            default:
                if (random().nextBoolean()) {
                    return random().nextFloat();
                } else {
                    return (random().nextInt(15) - 7) / 3f;
                }
        }
    }

    private static class FloatTestRange extends AbstractRange<Float> {
        float min;
        float max;

        FloatTestRange(float min, float max) {
            this.min = min;
            this.max = max;
        }

        @Override
        public Float getMin() {
            return min;
        }

        @Override
        protected void setMin(int dim, Object val) {
            assert dim == 0;
            float v = (Float) val;
            if (min < v) {
                max = v;
            } else {
                min = v;
            }
        }

        @Override
        public Float getMax() {
            return max;
        }

        @Override
        protected void setMax(int dim, Object val) {
            assert dim == 0;
            float v = (Float) val;
            if (max > v) {
                min = v;
            } else {
                max = v;
            }
        }

        @Override
        protected boolean isDisjoint(Range o) {
            FloatTestRange other = (FloatTestRange)o;
            return this.min > other.max || this.max < other.min;
        }

        @Override
        protected boolean isWithin(Range o) {
            FloatTestRange other = (FloatTestRange)o;
            if ((this.min >= other.min && this.max <= other.max) == false) {
                // not within:
                return false;
            }
            return true;
        }

        @Override
        protected boolean contains(Range o) {
            FloatTestRange other = (FloatTestRange) o;
            if ((this.min <= other.min && this.max >= other.max) == false) {
                // not contains:
                return false;
            }
            return true;
        }

    }

}
