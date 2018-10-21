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

import org.elasticsearch.index.mapper.RangeFieldMapper;

public class DoubleRandomBinaryDocValuesRangeQueryTests extends BaseRandomBinaryDocValuesRangeQueryTestCase {

    @Override
    protected String fieldName() {
        return "double_range_dv_field";
    }

    @Override
    protected RangeFieldMapper.RangeType rangeType() {
        return RangeFieldMapper.RangeType.DOUBLE;
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
