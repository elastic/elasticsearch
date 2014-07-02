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


package org.elasticsearch.search;

import org.apache.lucene.index.*;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;

import java.util.Locale;

/**
 * Defines what values to pick in the case a document contains multiple values for a particular field.
 */
public enum MultiValueMode {

    /**
     * Sum of all the values.
     */
    SUM {
        /**
         * Returns the sum of the two values
         */
        @Override
        public double apply(double a, double b) {
            return a + b;
        }

        /**
         * Returns the sum of the two values
         */
        @Override
        public long apply(long a, long b) {
            return a + b;
        }
    },

    /**
     * Average of all the values.
     */
    AVG {

        /**
         * Returns the sum of the two values
         */
        @Override
        public double apply(double a, double b) {
            return a + b;
        }

        /**
         * Returns the sum of the two values
         */
        @Override
        public long apply(long a, long b) {
            return a + b;
        }

        /**
         * Returns <code>a / Math.max(1.0d, numValues)</code>
         */
        @Override
        public double reduce(double a, int numValues) {
            return a / Math.max(1.0d, (double) numValues);
        }

        /**
         * Returns <code>Math.round(a / Math.max(1.0, numValues))</code>
         */
        @Override
        public long reduce(long a, int numValues) {
            return Math.round(a / Math.max(1.0, numValues));
        }
    },

    /**
     * Pick the lowest value.
     */
    MIN {
        /**
         * Equivalent to {@link Math#min(double, double)}
         */
        @Override
        public double apply(double a, double b) {
            return Math.min(a, b);
        }

        /**
         * Equivalent to {@link Math#min(long, long)}
         */
        @Override
        public long apply(long a, long b) {
            return Math.min(a, b);
        }

        /**
         * Returns {@link Double#POSITIVE_INFINITY}
         */
        @Override
        public double startDouble() {
            return Double.POSITIVE_INFINITY;
        }

        /**
         * Returns {@link Long#MAX_VALUE}
         */
        @Override
        public long startLong() {
            return Long.MAX_VALUE;
        }

        @Override
        protected long pick(SortedNumericDocValues values, long missingValue) {
            if (values.count() > 0) {
                return values.valueAt(0);
            } else {
                return missingValue;
            }
        }

        @Override
        protected double pick(SortedNumericDoubleValues values, double missingValue) {
            if (values.count() > 0) {
                return values.valueAt(0);
            } else {
                return missingValue;
            }
        }

        @Override
        protected BytesRef pick(SortedBinaryDocValues values, BytesRef missingValue) {
            if (values.count() > 0) {
                return values.valueAt(0);
            } else {
                return missingValue;
            }
        }

        @Override
        protected int pick(RandomAccessOrds values, int missingOrd) {
            if (values.cardinality() > 0) {
                return (int) values.ordAt(0);
            } else {
                return missingOrd;
            }
        }
    },

    /**
     * Pick the highest value.
     */
    MAX {
        /**
         * Equivalent to {@link Math#max(double, double)}
         */
        @Override
        public double apply(double a, double b) {
            return Math.max(a, b);
        }

        /**
         * Equivalent to {@link Math#max(long, long)}
         */
        @Override
        public long apply(long a, long b) {
            return Math.max(a, b);
        }

        /**
         * Returns {@link Double#NEGATIVE_INFINITY}
         */
        @Override
        public double startDouble() {
            return Double.NEGATIVE_INFINITY;
        }


        /**
         * Returns {@link Long#MIN_VALUE}
         */
        @Override
        public long startLong() {
            return Long.MIN_VALUE;
        }

        @Override
        protected long pick(SortedNumericDocValues values, long missingValue) {
            final int count = values.count();
            if (count > 0) {
                return values.valueAt(count - 1);
            } else {
                return missingValue;
            }
        }

        @Override
        protected double pick(SortedNumericDoubleValues values, double missingValue) {
            final int count = values.count();
            if (count > 0) {
                return values.valueAt(count - 1);
            } else {
                return missingValue;
            }
        }

        @Override
        protected BytesRef pick(SortedBinaryDocValues values, BytesRef missingValue) {
            final int count = values.count();
            if (count > 0) {
                return values.valueAt(count - 1);
            } else {
                return missingValue;
            }
        }

        @Override
        protected int pick(RandomAccessOrds values, int missingOrd) {
            final int count = values.cardinality();
            if (count > 0) {
                return (int) values.ordAt(count - 1);
            } else {
                return missingOrd;
            }
        }
    };

    /**
     * Applies the sort mode and returns the result. This method is meant to be
     * a binary function that is commonly used in a loop to find the relevant
     * value for the sort mode in a list of values. For instance if the sort mode
     * is {@link MultiValueMode#MAX} this method is equivalent to {@link Math#max(double, double)}.
     *
     * Note: all implementations are idempotent.
     *
     * @param a an argument
     * @param b another argument
     * @return the result of the function.
     */
    public abstract double apply(double a, double b);

    /**
     * Applies the sort mode and returns the result. This method is meant to be
     * a binary function that is commonly used in a loop to find the relevant
     * value for the sort mode in a list of values. For instance if the sort mode
     * is {@link MultiValueMode#MAX} this method is equivalent to {@link Math#max(long, long)}.
     *
     * Note: all implementations are idempotent.
     *
     * @param a an argument
     * @param b another argument
     * @return the result of the function.
     */
    public abstract long apply(long a, long b);

    /**
     * Returns an initial value for the sort mode that is guaranteed to have no impact if passed
     * to {@link #apply(double, double)}. This value should be used as the initial value if the
     * sort mode is applied to a non-empty list of values. For instance:
     * <pre>
     *     double relevantValue = sortMode.startDouble();
     *     for (int i = 0; i < array.length; i++) {
     *         relevantValue = sortMode.apply(array[i], relevantValue);
     *     }
     * </pre>
     *
     * Note: This method return <code>0</code> by default.
     *
     * @return an initial value for the sort mode.
     */
    public double startDouble() {
        return 0;
    }

    /**
     * Returns an initial value for the sort mode that is guaranteed to have no impact if passed
     * to {@link #apply(long, long)}. This value should be used as the initial value if the
     * sort mode is applied to a non-empty list of values. For instance:
     * <pre>
     *     long relevantValue = sortMode.startLong();
     *     for (int i = 0; i < array.length; i++) {
     *         relevantValue = sortMode.apply(array[i], relevantValue);
     *     }
     * </pre>
     *
     * Note: This method return <code>0</code> by default.
     * @return an initial value for the sort mode.
     */
    public long startLong() {
        return 0;
    }

    /**
     * Returns the aggregated value based on the sort mode. For instance if {@link MultiValueMode#AVG} is used
     * this method divides the given value by the number of values. The default implementation returns
     * the first argument.
     *
     * Note: all implementations are idempotent.
     */
    public double reduce(double a, int numValues) {
        return a;
    }

    /**
     * Returns the aggregated value based on the sort mode. For instance if {@link MultiValueMode#AVG} is used
     * this method divides the given value by the number of values. The default implementation returns
     * the first argument.
     *
     * Note: all implementations are idempotent.
     */
    public long reduce(long a, int numValues) {
        return a;
    }

    /**
     * A case insensitive version of {@link #valueOf(String)}
     *
     * @throws org.elasticsearch.ElasticsearchIllegalArgumentException if the given string doesn't match a sort mode or is <code>null</code>.
     */
    public static MultiValueMode fromString(String sortMode) {
        try {
            return valueOf(sortMode.toUpperCase(Locale.ROOT));
        } catch (Throwable t) {
            throw new ElasticsearchIllegalArgumentException("Illegal sort_mode " + sortMode);
        }
    }

    protected long pick(SortedNumericDocValues values, long missingValue) {
        final int count = values.count();
        if (count == 0) {
            return missingValue;
        } else {
            long aggregate = startLong();
            for (int i = 0; i < count; ++i) {
                aggregate = apply(aggregate, values.valueAt(i));
            }
            return reduce(aggregate, count);
        }
    }

    public NumericDocValues select(final SortedNumericDocValues values, final long missingValue) {
        final NumericDocValues singleton = DocValues.unwrapSingleton(values);
        if (singleton != null) {
            final Bits docsWithField = DocValues.unwrapSingletonBits(values);
            if (docsWithField == null || missingValue == 0) {
                return singleton;
            } else {
                return new NumericDocValues() {
                    @Override
                    public long get(int docID) {
                        final long value = singleton.get(docID);
                        if (value == 0 && docsWithField.get(docID) == false) {
                            return missingValue;
                        }
                        return value;
                    }
                };
            }
        } else {
            return new NumericDocValues() {
                @Override
                public long get(int docID) {
                    values.setDocument(docID);
                    return pick(values, missingValue);
                }
            };
        }
    }

    protected double pick(SortedNumericDoubleValues values, double missingValue) {
        final int count = values.count();
        if (count == 0) {
            return missingValue;
        } else {
            double aggregate = startDouble();
            for (int i = 0; i < count; ++i) {
                aggregate = apply(aggregate, values.valueAt(i));
            }
            return reduce(aggregate, count);
        }
    }

    public NumericDoubleValues select(final SortedNumericDoubleValues values, final double missingValue) {
        final NumericDoubleValues singleton = FieldData.unwrapSingleton(values);
        if (singleton != null) {
            final Bits docsWithField = FieldData.unwrapSingletonBits(values);
            if (docsWithField == null || missingValue == 0) {
                return singleton;
            } else {
                return new NumericDoubleValues() {
                    @Override
                    public double get(int docID) {
                        final double value = singleton.get(docID);
                        if (value == 0 && docsWithField.get(docID) == false) {
                            return missingValue;
                        }
                        return value;
                    }
                };
            }
        } else {
            return new NumericDoubleValues() {
                @Override
                public double get(int docID) {
                    values.setDocument(docID);
                    return pick(values, missingValue);
                }
            };
        }
    }

    protected BytesRef pick(SortedBinaryDocValues values, BytesRef missingValue) {
        throw new ElasticsearchIllegalArgumentException("Unsupported sort mode: " + this);
    }

    public BinaryDocValues select(final SortedBinaryDocValues values, final BytesRef missingValue) {
        final BinaryDocValues singleton = FieldData.unwrapSingleton(values);
        if (singleton != null) {
            final Bits docsWithField = FieldData.unwrapSingletonBits(values);
            if (docsWithField == null || missingValue == null || missingValue.length == 0) {
                return singleton;
            } else {
                return new BinaryDocValues() {
                    @Override
                    public BytesRef get(int docID) {
                        final BytesRef value = singleton.get(docID);
                        if (value.length == 0 && docsWithField.get(docID) == false) {
                            return missingValue;
                        }
                        return value;
                    }
                };
            }
        } else {
            return new BinaryDocValues() {
                @Override
                public BytesRef get(int docID) {
                    values.setDocument(docID);
                    return pick(values, missingValue);
                }
            };
        }
    }

    protected int pick(RandomAccessOrds values, int missingOrd) {
        throw new ElasticsearchIllegalArgumentException("Unsupported sort mode: " + this);
    }

    public SortedDocValues select(final RandomAccessOrds values, final int missingOrd) {
        if (values.getValueCount() >= Integer.MAX_VALUE) {
            throw new UnsupportedOperationException("fields containing more than " + (Integer.MAX_VALUE-1) + " unique terms are unsupported");
        }

        final SortedDocValues singleton = DocValues.unwrapSingleton(values);
        if (singleton != null) {
            if (missingOrd == -1) {
                return singleton;
            } else {
                return new SortedDocValues() {
                    @Override
                    public int getOrd(int docID) {
                        final int ord = singleton.getOrd(docID);
                        if (ord == -1) {
                            return missingOrd;
                        }
                        return ord;
                    }

                    @Override
                    public BytesRef lookupOrd(int ord) {
                        return values.lookupOrd(ord);
                    }

                    @Override
                    public int getValueCount() {
                        return (int) values.getValueCount();
                    }
                };
            }
        } else {
            return new SortedDocValues() {
                @Override
                public int getOrd(int docID) {
                    values.setDocument(docID);
                    return pick(values, missingOrd);
                }

                @Override
                public BytesRef lookupOrd(int ord) {
                    return values.lookupOrd(ord);
                }

                @Override
                public int getValueCount() {
                    return (int) values.getValueCount();
                }
            };
        }
    }

}
