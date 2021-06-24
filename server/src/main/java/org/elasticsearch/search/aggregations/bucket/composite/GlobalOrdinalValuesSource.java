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
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.Releasables;
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
    private static final long NULL_ORD = Long.MIN_VALUE;

    private final CheckedFunction<LeafReaderContext, SortedSetDocValues, IOException> docValuesFunc;

    // ordinals, which are remapped whenever we visit a new segment.
    // Entries might be Long.MIN_VALUE to represent the "null" value is null,
    // or negative when the corresponding term is not known to current lookup
    private LongArray valuesOrd;
    // we cache some values, namely when the corresponding term is not known to current lookup
    // (i.e. corresponding entry in valuesOrd is negative but not Long.MIN_VALUE)
    private ObjectArray<BytesRef> values;
    private ObjectArray<SortedSetDocValues> valuesLookup;

    // might be Long.MIN_VALUE when the value is null, or negative if the term is not known to current lookup
    private Long currentValueOrd;
    // when term is not known to current lookup, we have the proper value here
    private BytesRef currentValue;
    private SortedSetDocValues currentValueLookup;

    // might be Long.MIN_VALUE when the value is null, or negative if the term is not known to current lookup
    // when term is not known to current lookup, then we still have term in afterValue
    private Long afterValueOrd;
    private SortedSetDocValues afterValueLookup;

//    private long lastLookupOrd = -1;
//    private BytesRef lastLookupValue;
    private SortedSetDocValues lookup;

    private int numSlots = 0;

    GlobalOrdinalValuesSource(BigArrays bigArrays, MappedFieldType type,
                              CheckedFunction<LeafReaderContext, SortedSetDocValues, IOException> docValuesFunc,
                              DocValueFormat format, boolean missingBucket, int size, int reverseMul) {
        super(bigArrays, format, type, missingBucket, size, reverseMul);
        this.docValuesFunc = docValuesFunc;
        this.valuesOrd = bigArrays.newLongArray(Math.min(size, 100), false);
        this.valuesLookup = bigArrays.newObjectArray(Math.min(size, 100));
        this.values = bigArrays.newObjectArray(Math.min(size, 100));
    }

    @Override
    void copyCurrent(int slot) {
        numSlots = Math.max(numSlots, slot + 1);
        valuesOrd = bigArrays.grow(valuesOrd, numSlots);
        values = bigArrays.grow(values, numSlots);
        valuesLookup = bigArrays.grow(valuesLookup, numSlots);

        assert currentValueOrd != null && (currentValueOrd == Long.MIN_VALUE || currentValueOrd >= 0);
        valuesOrd.set(slot, currentValueOrd);
        assert currentValue == null;
        values.set(slot, currentValue); // reset values as we only want to keep it for entry
        assert currentValueLookup != null;
        assert lookup == currentValueLookup;
        valuesLookup.set(slot, currentValueLookup);
    }

    @Override
    int compare(int from, int to) {
        assert valuesLookup.get(from) == valuesLookup.get(to);
        return compareInternal(valuesOrd.get(from), valuesOrd.get(to), values.get(from), values.get(to));
    }

    @Override
    int compareCurrent(int slot) {
        assert valuesLookup.get(slot) == currentValueLookup;
        assert currentValueOrd != null;
        return compareInternal(currentValueOrd, valuesOrd.get(slot), currentValue, values.get(slot));
    }

    @Override
    int compareCurrentWithAfter() {
        assert currentValueLookup == afterValueLookup;
        assert currentValueOrd != null && afterValueOrd != null;
        return compareInternal(currentValueOrd, afterValueOrd, currentValue, afterValue);
    }

    @Override
    int hashCode(int slot) {
        return Long.hashCode(valuesOrd.get(slot));
    }

    @Override
    int hashCodeCurrent() {
        assert currentValueOrd != null;
        return Long.hashCode(currentValueOrd);
    }

    int compareInternal(long ord1, long ord2, BytesRef fallback1, BytesRef fallback2) {
        if (ord1 == Long.MIN_VALUE || ord2 == Long.MIN_VALUE) {
            if (ord1 == Long.MIN_VALUE && ord2 == Long.MIN_VALUE) {
                return 0;
            } else if (ord1 == Long.MIN_VALUE) {
                return Long.compare(-1L, ord2) * reverseMul;
            } else {
                assert ord2 == Long.MIN_VALUE;
                return Long.compare(ord1, -1L) * reverseMul;
            }
        } else if (ord1 < 0) {
            if (ord2 < 0) {
                int cmp = Long.compare(-ord1 - 1, -ord2 - 1);
                if (cmp == 0) {
                    assert fallback1 != null && fallback2 != null;
                    return fallback1.compareTo(fallback2) * reverseMul;
                }
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
            afterValueOrd = null;
        } else {
            throw new IllegalArgumentException("invalid value, expected string, got " + value.getClass().getSimpleName());
        }
    }

    @Override
    BytesRef toComparable(int slot) throws IOException {
        long ord = valuesOrd.get(slot);
        if (missingBucket && ord == Long.MIN_VALUE) {
            return null;
        /*} else if (ord == lastLookupOrd) {
            return lastLookupValue;*/
        } else if (ord < 0L) {
            assert ord != Long.MIN_VALUE;
            return values.get(slot);
        } else {
            assert ord >= 0L;
            // TODO: reintroduce and fix lastLookupOrd / lastLookupValue
            //lastLookupOrd = ord;
//            lastLookupValue = BytesRef.deepCopyOf(lookups.get(slot).lookupOrd(ord));
//            return lastLookupValue;
            return BytesRef.deepCopyOf(valuesLookup.get(slot).lookupOrd(ord));
        }
    }

    @Override
    LeafBucketCollector getLeafCollector(LeafReaderContext context, LeafBucketCollector next) throws IOException {
        final SortedSetDocValues dvs = docValuesFunc.apply(context);
        remapOrdinals(lookup, dvs);
        lookup = dvs;
        return new LeafBucketCollector() {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                assert dvs == lookup;
                if (dvs.advanceExact(doc)) {
                    long ord;
                    while ((ord = dvs.nextOrd()) != NO_MORE_ORDS) {
                        currentValueOrd = ord;
                        currentValue = null;
                        currentValueLookup = dvs;
                        next.collect(doc, bucket);
                    }
                } else if (missingBucket) {
                    currentValueOrd = Long.MIN_VALUE;
                    currentValue = null;
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
        remapOrdinals(lookup, dvs);
        lookup = dvs;
        return new LeafBucketCollector() {
            boolean currentValueIsSet = false;

            @Override
            public void collect(int doc, long bucket) throws IOException {
                assert dvs == lookup;
                if (currentValueIsSet == false) {
                    if (dvs.advanceExact(doc)) {
                        long ord;
                        while ((ord = dvs.nextOrd()) != NO_MORE_ORDS) {
                            if (term.equals(dvs.lookupOrd(ord))) {
                                currentValueIsSet = true;
                                currentValueOrd = ord;
                                currentValue = null;
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

    /**
     * Remaps ordinals when switching LeafReaders. It's possible that a term is not mapped for the new LeafReader,
     * in that case remember the term so that future remapping steps can accurately be done.
     */
    private void remapOrdinals(SortedSetDocValues oldMapping, SortedSetDocValues newMapping) throws IOException {
        assert currentValueLookup == oldMapping;
        if (currentValueOrd != null) {
            if (currentValueOrd == Long.MIN_VALUE) {
                currentValue = null;
            } else if (currentValueOrd < 0) {
                // this wasn't set in last leafreader, so use previous value for lookup
                assert currentValue != null;
            } else {
                currentValue = BytesRef.deepCopyOf(currentValueLookup.lookupOrd(currentValueOrd));
                assert currentValue != null;
            }
            if (currentValue == null) {
                currentValueOrd = Long.MIN_VALUE;
            } else {
                currentValueOrd = newMapping.lookupTerm(currentValue);
            }
        }
        currentValueLookup = newMapping;

        assert afterValueLookup == oldMapping;
        if (afterValue != null) {
            afterValueOrd = newMapping.lookupTerm(afterValue);
        }
        afterValueLookup = newMapping;

        for (int i = 0; i < numSlots; i++) {
            assert valuesLookup.get(i) == oldMapping;
            long ord = valuesOrd.get(i);
            if (ord == Long.MIN_VALUE) {
                values.set(i, null);
            } else if (ord < 0) {
                // this wasn't set in last leafreader, so use previous value for lookup
                assert values.get(i) != null;
            } else {
                values.set(i, BytesRef.deepCopyOf(valuesLookup.get(i).lookupOrd(ord)));
                assert values.get(i) != null;
            }
            if (values.get(i) == null) {
                valuesOrd.set(i, Long.MIN_VALUE);
            } else {
                valuesOrd.set(i, newMapping.lookupTerm(values.get(i)));
            }
            valuesLookup.set(i, newMapping);
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
        Releasables.close(valuesOrd);
    }
}
