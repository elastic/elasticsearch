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
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.StringFieldType;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.LeafBucketCollector;

import java.io.IOException;

import static org.apache.lucene.index.SortedSetDocValues.NO_MORE_ORDS;

/**
 * A {@link SingleDimensionValuesSource} for global ordinals.
 */
class GlobalOrdinalValuesSource extends SingleDimensionValuesSource<BytesRef> {
    private final CheckedFunction<LeafReaderContext, SortedSetDocValues, IOException> docValuesFunc;
    private LongArray values;
    private ObjectArray<BytesRef> bytesRefValues;
    private ObjectArray<BytesRefBuilder> bytesRefValuesBuilders;
    private ObjectArray<SortedSetDocValues> lookups;
    private long currentValueOrd;
    private SortedSetDocValues currentValueLookup;
    private Long afterValueOrd;
    private SortedSetDocValues afterValueLookup;

    private long lastLookupOrd = -1;
    private BytesRef lastLookupValue;
    private SortedSetDocValues lookup;

    private int slots = 0;

    GlobalOrdinalValuesSource(BigArrays bigArrays, MappedFieldType type,
                              CheckedFunction<LeafReaderContext, SortedSetDocValues, IOException> docValuesFunc,
                              DocValueFormat format, boolean missingBucket, int size, int reverseMul) {
        super(bigArrays, format, type, missingBucket, size, reverseMul);
        this.docValuesFunc = docValuesFunc;
        this.values = bigArrays.newLongArray(Math.min(size, 100), false);
        this.lookups = bigArrays.newObjectArray(Math.min(size, 100));
        this.bytesRefValues = bigArrays.newObjectArray(Math.min(size, 100));
        this.bytesRefValuesBuilders = bigArrays.newObjectArray(Math.min(size, 100));
    }

    @Override
    void copyCurrent(int slot) {
        values = bigArrays.grow(values, slot+1);
        lookups = bigArrays.grow(lookups, slot+1);
        bytesRefValues = bigArrays.grow(bytesRefValues, slot+1);
        bytesRefValuesBuilders = bigArrays.grow(bytesRefValuesBuilders, slot+1);

        values.set(slot, currentValueOrd);
        assert currentValueLookup != null;
        lookups.set(slot, currentValueLookup);
        bytesRefValues.set(slot, null);
        slots = Math.max(slots, slot + 1);
    }

    @Override
    int compare(int from, int to) {
        return compareInternal(values.get(from), values.get(to));
    }

    @Override
    int compareCurrent(int slot) {
        return compareInternal(currentValueOrd, values.get(slot));
    }

    @Override
    int compareCurrentWithAfter() {
        return compareInternal(currentValueOrd, afterValueOrd);
    }

    @Override
    int hashCode(int slot) {
        return Long.hashCode(values.get(slot));
    }

    @Override
    int hashCodeCurrent() {
        return Long.hashCode(currentValueOrd);
    }

    int compareInternal(long ord1, long ord2) {
        if (ord1 == Long.MIN_VALUE || ord2 == Long.MIN_VALUE) {
            if (ord1 == Long.MIN_VALUE && ord2 == Long.MIN_VALUE) {
                return 0;
            }
            if (ord1 == Long.MIN_VALUE) {
                return Long.compare(-1L, ord2) * reverseMul;
            }
            if (ord2 == Long.MIN_VALUE) {
                return Long.compare(ord1, -1L) * reverseMul;
            }
        }
        if (ord1 < 0) {
            if (ord2 < 0) {
                int cmp = Long.compare(-ord1 - 1, -ord2 - 1);
                return cmp * reverseMul;
            } else {
                int cmp = Long.compare(-ord1 - 1, ord2);
                if (cmp == 0) {
                    return -1 * reverseMul;
                }
                return cmp * reverseMul;
            }
        } else {
            if (ord2 < 0) {
                int cmp = Long.compare(ord1, -ord2 - 1);
                if (cmp == 0) {
                    return reverseMul;
                }
                return cmp * reverseMul;
            } else {
                return Long.compare(ord1, ord2) * reverseMul;
            }
        }
    }

    @Override
    void setAfter(Comparable value) {
        if (missingBucket && value == null) {
            afterValue = null;
            afterValueOrd = Long.MIN_VALUE;
        } else if (value.getClass() == String.class || (missingBucket && fieldType == null)) {
            // the value might be not string if this field is missing in this shard but present in other shards
            // and doesn't have a string type
            afterValue = format.parseBytesRef(value.toString());
        } else {
            throw new IllegalArgumentException("invalid value, expected string, got " + value.getClass().getSimpleName());
        }
    }

    @Override
    BytesRef toComparable(int slot) throws IOException {
        long ord = values.get(slot);
        if (missingBucket && ord == Long.MIN_VALUE) {
            return null;
        /*} else if (ord == lastLookupOrd) {
            return lastLookupValue;*/
        } else if (ord < 0L) {
            return bytesRefValues.get(slot);
        } else {
            assert ord >= 0L;
            lastLookupOrd = ord;
            lastLookupValue = BytesRef.deepCopyOf(lookups.get(slot).lookupOrd(ord));
            return lastLookupValue;
        }
    }

    @Override
    LeafBucketCollector getLeafCollector(LeafReaderContext context, LeafBucketCollector next) throws IOException {
        final SortedSetDocValues dvs = docValuesFunc.apply(context);
        remap(lookup, dvs);
        lookup = dvs;
        return new LeafBucketCollector() {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (dvs.advanceExact(doc)) {
                    long ord;
                    while ((ord = dvs.nextOrd()) != NO_MORE_ORDS) {
                        currentValueOrd = ord;
                        currentValueLookup = dvs;
                        next.collect(doc, bucket);
                    }
                } else if (missingBucket) {
                    currentValueOrd = Long.MIN_VALUE;
                    currentValueLookup = dvs;
                    next.collect(doc, bucket);
                }
            }
        };
    }

    @Override
    LeafBucketCollector getLeafCollector(Comparable value, LeafReaderContext context, LeafBucketCollector next) throws IOException {
        if (value.getClass() != BytesRef.class) {
            throw new IllegalArgumentException("Expected BytesRef, got " + value.getClass());
        }
        BytesRef term = (BytesRef) value;
        final SortedSetDocValues dvs = docValuesFunc.apply(context);
        remap(lookup, dvs);
        lookup = dvs;
        return new LeafBucketCollector() {
            boolean currentValueIsSet = false;

            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (currentValueIsSet == false) {
                    if (dvs.advanceExact(doc)) {
                        long ord;
                        while ((ord = dvs.nextOrd()) != NO_MORE_ORDS) {
                            if (term.equals(dvs.lookupOrd(ord))) {
                                currentValueIsSet = true;
                                currentValueOrd = ord;
                                currentValueLookup = dvs;
                                break;
                            }
                        }
                    }
                }
                assert currentValueIsSet;
                next.collect(doc, bucket);
            }
        };
    }

    private void remap(SortedSetDocValues oldMapping, SortedSetDocValues newMapping) throws IOException {
        assert currentValueLookup == null || currentValueLookup == oldMapping;
        // only remap an actual value
        if (currentValueLookup != null) {
            if (currentValueOrd != Long.MIN_VALUE) {
                currentValueOrd = newMapping.lookupTerm(currentValueLookup.lookupOrd(currentValueOrd));
            }
            currentValueLookup = newMapping;
        }

        assert afterValueLookup == null || afterValueLookup == oldMapping;
        if (afterValue != null) {
            afterValueOrd = newMapping.lookupTerm(afterValue);
            afterValueLookup = newMapping;
        }

        for (int i = 0; i < slots; i++) {
            assert lookups.get(i) == oldMapping;
            long ord = values.get(i);
            if (ord == Long.MIN_VALUE) {
                bytesRefValues.set(i, null);
            } else if (ord < 0) {
                // this wasn't set in last leafreader, so use previous value for lookup
                int x = 0;
            } else {
                bytesRefValues.set(i, BytesRef.deepCopyOf(lookups.get(i).lookupOrd(ord)));
            }
            if (bytesRefValues.get(i) == null) {
                values.set(i, Long.MIN_VALUE);
            } else {
                values.set(i, newMapping.lookupTerm(bytesRefValues.get(i)));
            }
            lookups.set(i, newMapping);
        }
    }

    @Override
    SortedDocsProducer createSortedDocsProducerOrNull(IndexReader reader, Query query) {
        if (checkIfSortedDocsIsApplicable(reader, fieldType) == false ||
                fieldType instanceof StringFieldType == false ||
                    (query != null && query.getClass() != MatchAllDocsQuery.class)) {
            return null;
        }
        return new TermsSortedDocsProducer(fieldType.name());
    }

    @Override
    public void close() {
        Releasables.close(values);
    }
}
