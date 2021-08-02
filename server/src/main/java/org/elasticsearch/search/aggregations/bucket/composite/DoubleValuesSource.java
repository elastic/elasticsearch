/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.composite;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.LeafBucketCollector;

import java.io.IOException;

/**
 * A {@link SingleDimensionValuesSource} for doubles.
 */
class DoubleValuesSource extends SingleDimensionValuesSource<Double> {
    private final CheckedFunction<LeafReaderContext, SortedNumericDoubleValues, IOException> docValuesFunc;
    private final BitArray bits;
    private DoubleArray values;
    private double currentValue;
    private boolean missingCurrentValue;

    DoubleValuesSource(BigArrays bigArrays, MappedFieldType fieldType,
                       CheckedFunction<LeafReaderContext, SortedNumericDoubleValues, IOException> docValuesFunc,
                       DocValueFormat format, boolean missingBucket, int size, int reverseMul) {
        super(bigArrays, format, fieldType, missingBucket, size, reverseMul);
        this.docValuesFunc = docValuesFunc;
        this.bits = missingBucket ? new BitArray(100, bigArrays) : null;
        this.values = bigArrays.newDoubleArray(Math.min(size, 100), false);
    }

    @Override
    void copyCurrent(int slot) {
        values = bigArrays.grow(values, slot+1);
        if (missingBucket && missingCurrentValue) {
            bits.clear(slot);
        } else {
            assert missingCurrentValue == false;
            if (missingBucket) {
                bits.set(slot);
            }
            values.set(slot, currentValue);
        }
    }

    @Override
    int compare(int from, int to) {
        if (missingBucket) {
            if (bits.get(from) == false) {
                return bits.get(to) ? -1 * reverseMul : 0;
            } else if (bits.get(to) == false) {
                return reverseMul;
            }
        }
        return compareValues(values.get(from), values.get(to));
    }

    @Override
    int compareCurrent(int slot) {
        if (missingBucket) {
            if (missingCurrentValue) {
                return bits.get(slot) ? -1 * reverseMul : 0;
            } else if (bits.get(slot) == false) {
                return reverseMul;
            }
        }
        return compareValues(currentValue, values.get(slot));
    }

    @Override
    int compareCurrentWithAfter() {
        if (missingBucket) {
            if (missingCurrentValue) {
                return afterValue != null ? -1 * reverseMul : 0;
            } else if (afterValue == null) {
                return reverseMul;
            }
        }
        return compareValues(currentValue, afterValue);
    }

    @Override
    int hashCode(int slot) {
        if (missingBucket && bits.get(slot) == false) {
            return 0;
        } else {
            return Double.hashCode(values.get(slot));
        }
    }

    @Override
    int hashCodeCurrent() {
        if (missingCurrentValue) {
            return 0;
        } else {
            return Double.hashCode(currentValue);
        }
    }

    private int compareValues(double v1, double v2) {
        return Double.compare(v1, v2) * reverseMul;
    }

    @Override
    void setAfter(Comparable<?> value) {
        if (missingBucket && value == null) {
            afterValue = null;
        } else if (value instanceof Number) {
            afterValue = ((Number) value).doubleValue();
        } else {
            afterValue = format.parseDouble(value.toString(), false, () -> {
                throw new IllegalArgumentException("now() is not supported in [after] key");
            });
        }
    }

    @Override
    Double toComparable(int slot) {
        if (missingBucket && bits.get(slot) == false) {
            return null;
        }
        assert missingBucket == false || bits.get(slot);
        return values.get(slot);
    }

    @Override
    LeafBucketCollector getLeafCollector(LeafReaderContext context, LeafBucketCollector next) throws IOException {
        final SortedNumericDoubleValues dvs = docValuesFunc.apply(context);
        return new LeafBucketCollector() {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (dvs.advanceExact(doc)) {
                    int num = dvs.docValueCount();
                    for (int i = 0; i < num; i++) {
                        currentValue = dvs.nextValue();
                        missingCurrentValue = false;
                        next.collect(doc, bucket);
                    }
                } else if (missingBucket) {
                    missingCurrentValue = true;
                    next.collect(doc, bucket);
                }
            }
        };
    }

    @Override
    LeafBucketCollector getLeafCollector(Comparable<Double> value, LeafReaderContext context, LeafBucketCollector next) {
        if (value.getClass() != Double.class) {
            throw new IllegalArgumentException("Expected Double, got " + value.getClass());
        }
        currentValue = (Double) value;
        return new LeafBucketCollector() {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                next.collect(doc, bucket);
            }
        };
    }

    @Override
    SortedDocsProducer createSortedDocsProducerOrNull(IndexReader reader, Query query) {
        return null;
    }

    @Override
    public void close() {
        Releasables.close(values, bits);
    }
}
