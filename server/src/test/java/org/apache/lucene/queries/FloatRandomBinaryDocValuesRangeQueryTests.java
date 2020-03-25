/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
