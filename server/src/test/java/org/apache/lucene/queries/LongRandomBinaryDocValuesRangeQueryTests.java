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

import org.apache.lucene.util.TestUtil;
import org.elasticsearch.index.mapper.RangeType;

public class LongRandomBinaryDocValuesRangeQueryTests extends BaseRandomBinaryDocValuesRangeQueryTestCase {

    @Override
    protected String fieldName() {
        return "long_range_dv_field";
    }

    @Override
    protected RangeType rangeType() {
        return RangeType.LONG;
    }

    @Override
    protected Range nextRange(int dimensions) throws Exception {
        long value1 = nextLongInternal();
        long value2 = nextLongInternal();
        long min = Math.min(value1, value2);
        long max = Math.max(value1, value2);
        return new LongTestRange(min, max);
    }

    private long nextLongInternal() {
        switch (random().nextInt(5)) {
            case 0:
                return Long.MIN_VALUE;
            case 1:
                return Long.MAX_VALUE;
            default:
                int bpv = random().nextInt(64);
                switch (bpv) {
                    case 64:
                        return random().nextLong();
                    default:
                        long v = TestUtil.nextLong(random(), 0, (1L << bpv) - 1);
                        if (bpv > 0) {
                            // negative values sometimes
                            v -= 1L << (bpv - 1);
                        }
                        return v;
                }
        }
    }

    private static class LongTestRange extends AbstractRange<Long> {
        long min;
        long max;

        LongTestRange(long min, long max) {
            this.min = min;
            this.max = max;
        }

        @Override
        public Long getMin() {
            return min;
        }

        @Override
        protected void setMin(int dim, Object val) {
            assert dim == 0;
            long v = (Long)val;
            if (min < v) {
                max = v;
            } else {
                min = v;
            }
        }

        @Override
        public Long getMax() {
            return max;
        }

        @Override
        protected void setMax(int dim, Object val) {
            assert dim == 0;
            long v = (Long)val;
            if (max > v) {
                min = v;
            } else {
                max = v;
            }
        }

        @Override
        protected boolean isDisjoint(Range o) {
            LongTestRange other = (LongTestRange)o;
            return this.min > other.max || this.max < other.min;
        }

        @Override
        protected boolean isWithin(Range o) {
            LongTestRange other = (LongTestRange)o;
            if ((this.min >= other.min && this.max <= other.max) == false) {
                // not within:
                return false;
            }
            return true;
        }

        @Override
        protected boolean contains(Range o) {
            LongTestRange other = (LongTestRange) o;
            if ((this.min <= other.min && this.max >= other.max) == false) {
                // not contains:
                return false;
            }
            return true;
        }

    }

}
