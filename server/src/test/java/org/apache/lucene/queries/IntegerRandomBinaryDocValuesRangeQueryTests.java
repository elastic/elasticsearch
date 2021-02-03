/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.apache.lucene.queries;

import org.apache.lucene.util.TestUtil;
import org.elasticsearch.index.mapper.RangeType;

public class IntegerRandomBinaryDocValuesRangeQueryTests extends BaseRandomBinaryDocValuesRangeQueryTestCase {

    @Override
    protected String fieldName() {
        return "int_range_dv_field";
    }

    @Override
    protected RangeType rangeType() {
        return RangeType.INTEGER;
    }

    @Override
    protected Range nextRange(int dimensions) throws Exception {
        int value1 = nextIntInternal();
        int value2 = nextIntInternal();
        int min = Math.min(value1, value2);
        int max = Math.max(value1, value2);
        return new IntTestRange(min, max);
    }

    private int nextIntInternal() {
        switch (random().nextInt(5)) {
            case 0:
                return Integer.MIN_VALUE;
            case 1:
                return Integer.MAX_VALUE;
            default:
                int bpv = random().nextInt(32);
                switch (bpv) {
                    case 32:
                        return random().nextInt();
                    default:
                        int v = TestUtil.nextInt(random(), 0, (1 << bpv) - 1);
                        if (bpv > 0) {
                            // negative values sometimes
                            v -= 1 << (bpv - 1);
                        }
                        return v;
                }
        }
    }

    private static class IntTestRange extends AbstractRange<Integer> {
        int min;
        int max;

        IntTestRange(int min, int max) {
            this.min = min;
            this.max = max;
        }

        @Override
        public Integer getMin() {
            return min;
        }

        @Override
        protected void setMin(int dim, Object val) {
            assert dim == 0;
            int v = (Integer) val;
            if (min < v) {
                max = v;
            } else {
                min = v;
            }
        }

        @Override
        public Integer getMax() {
            return max;
        }

        @Override
        protected void setMax(int dim, Object val) {
            assert dim == 0;
            int v = (Integer) val;
            if (max > v) {
                min = v;
            } else {
                max = v;
            }
        }

        @Override
        protected boolean isDisjoint(Range o) {
            IntTestRange other = (IntTestRange)o;
            return this.min > other.max || this.max < other.min;
        }

        @Override
        protected boolean isWithin(Range o) {
            IntTestRange other = (IntTestRange)o;
            if ((this.min >= other.min && this.max <= other.max) == false) {
                // not within:
                return false;
            }
            return true;
        }

        @Override
        protected boolean contains(Range o) {
            IntTestRange other = (IntTestRange) o;
            if ((this.min <= other.min && this.max >= other.max) == false) {
                // not contains:
                return false;
            }
            return true;
        }

    }

}
