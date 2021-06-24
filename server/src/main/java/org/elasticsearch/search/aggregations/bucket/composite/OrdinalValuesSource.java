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
import java.util.function.LongConsumer;

import static org.apache.lucene.index.SortedSetDocValues.NO_MORE_ORDS;

/**
 * A {@link SingleDimensionValuesSource} for ordinals.
 */
class OrdinalValuesSource extends SingleDimensionValuesSource<BytesRef> {
    private final LongConsumer breakerConsumer;
    private final CheckedFunction<LeafReaderContext, SortedSetDocValues, IOException> docValuesFunc;

    // ordinals, which are remapped whenever we visit a new segment.
    // Entries might be Long.MIN_VALUE to represent the missing bucket,
    // or negative when the corresponding term is not known to the current lookup
    private LongArray valuesOrd;
    // when term is not known to current lookup, then the term from a previous lookup is stored here, else null
    private ObjectArray<BytesRef> values;
    // number of slots in the above arrays that contain any actual values
    private int numSlots = 0;


    // is Long.MIN_VALUE when the value represents the missing bucket, or negative if the term is not known to current lookup
    private Long currentValueOrd;
    // when term is not known to current lookup, then the term from a previous lookup is stored here, else null
    private BytesRef currentValue;

    // is Long.MIN_VALUE when the value represents the missing bucket, or negative if the term is not known to current lookup
    // when term is not known to current lookup, then the term from a previous lookup is stored in afterValue, else afterValue is null
    private Long afterValueOrd;

    // small cache to avoid repeated lookups in toComparable
    private Long lastLookupOrd;
    private BytesRef lastLookupValue;

    // current lookup
    private SortedSetDocValues lookup;

    OrdinalValuesSource(BigArrays bigArrays, LongConsumer breakerConsumer, MappedFieldType type,
                        CheckedFunction<LeafReaderContext, SortedSetDocValues, IOException> docValuesFunc,
                        DocValueFormat format, boolean missingBucket, int size, int reverseMul) {
        super(bigArrays, format, type, missingBucket, size, reverseMul);
        this.breakerConsumer = breakerConsumer;
        this.docValuesFunc = docValuesFunc;
        this.valuesOrd = bigArrays.newLongArray(Math.min(size, 100), false);
        this.values = bigArrays.newObjectArray(Math.min(size, 100));
    }

    @Override
    void copyCurrent(int slot) {
        numSlots = Math.max(numSlots, slot + 1);
        valuesOrd = bigArrays.grow(valuesOrd, numSlots);
        values = bigArrays.grow(values, numSlots);

        assert currentValueOrd != null && (currentValueOrd == Long.MIN_VALUE || currentValueOrd >= 0);
        valuesOrd.set(slot, currentValueOrd);
        assert currentValue == null;
        BytesRef previousValue = values.get(slot);
        if (previousValue != null) {
            // some bytes possibly freed
            breakerConsumer.accept(- previousValue.bytes.length);
        }
        values.set(slot, null);
    }

    @Override
    int compare(int from, int to) {
        assert from < numSlots && to < numSlots;
        return compareInternal(valuesOrd.get(from), valuesOrd.get(to), values.get(from), values.get(to)) * reverseMul;
    }

    @Override
    int compareCurrent(int slot) {
        assert currentValueOrd != null;
        assert slot < numSlots;
        return compareInternal(currentValueOrd, valuesOrd.get(slot), currentValue, values.get(slot)) * reverseMul;
    }

    @Override
    int compareCurrentWithAfter() {
        assert currentValueOrd != null && afterValueOrd != null;
        return compareInternal(currentValueOrd, afterValueOrd, currentValue, afterValue) * reverseMul;
    }

    @Override
    int hashCode(int slot) {
        assert slot < numSlots;
        return Long.hashCode(valuesOrd.get(slot));
    }

    @Override
    int hashCodeCurrent() {
        assert currentValueOrd != null;
        return Long.hashCode(currentValueOrd);
    }

    private int compareInternal(long ord1, long ord2, BytesRef bytesRef1, BytesRef bytesRef2) {
        if (ord1 >= 0 && ord2 >= 0) {
            return Long.compare(ord1, ord2);
        } else if (ord1 == Long.MIN_VALUE || ord2 == Long.MIN_VALUE) {
            return Long.compare(ord1, ord2);
        } else if (ord1 < 0 && ord2 < 0) {
            if (ord1 == ord2) {
                // we need to compare actual terms to properly order
                assert bytesRef1 != null && bytesRef2 != null;
                return bytesRef1.compareTo(bytesRef2);
            }
            return Long.compare(-ord1 - 1, -ord2 - 1);
        } else {
            if (ord1 < 0) {
                assert ord1 < 0 && ord2 >= 0;
                int cmp = Long.compare(-ord1 - 1, ord2);
                if (cmp == 0) {
                    return -1;
                }
                return cmp;
            } else {
                assert ord1 >= 0 && ord2 < 0;
                int cmp = Long.compare(ord1, -ord2 - 1);
                if (cmp == 0) {
                    return 1;
                }
                return cmp;
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
        assert slot < numSlots;
        long ord = valuesOrd.get(slot);
        if (ord == Long.MIN_VALUE) {
            assert missingBucket;
            return null;
        } else if (ord < 0) {
            return values.get(slot);
        } else if (lastLookupOrd != null && ord == lastLookupOrd) {
            assert ord >= 0;
            return lastLookupValue;
        } else {
            assert ord >= 0;
            lastLookupOrd = ord;
            return lastLookupValue = BytesRef.deepCopyOf(lookup.lookupOrd(ord));
        }
    }

    @Override
    LeafBucketCollector getLeafCollector(LeafReaderContext context, LeafBucketCollector next) throws IOException {
        final SortedSetDocValues dvs = docValuesFunc.apply(context);
        remapOrdinals(dvs);
        return new LeafBucketCollector() {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                assert dvs == lookup;
                if (dvs.advanceExact(doc)) {
                    long ord;
                    while ((ord = dvs.nextOrd()) != NO_MORE_ORDS) {
                        currentValueOrd = ord;
                        currentValue = null;
                        next.collect(doc, bucket);
                    }
                } else if (missingBucket) {
                    currentValueOrd = Long.MIN_VALUE;
                    currentValue = null;
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
        remapOrdinals(dvs);
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
    private void remapOrdinals(SortedSetDocValues newMapping) throws IOException {
        if (currentValueOrd != null) {
            if (currentValueOrd == Long.MIN_VALUE) {
                currentValue = null;
            } else if (currentValueOrd < 0) {
                // this wasn't set in last leafreader, so use previous value for lookup
                assert currentValue != null;
            } else {
                currentValue = BytesRef.deepCopyOf(lookup.lookupOrd(currentValueOrd));
                assert currentValue != null;
            }
            if (currentValue == null) {
                currentValueOrd = Long.MIN_VALUE;
            } else {
                currentValueOrd = newMapping.lookupTerm(currentValue);
            }
        }

        if (afterValue != null) {
            afterValueOrd = newMapping.lookupTerm(afterValue);
        }

        for (int i = 0; i < numSlots; i++) {
            long ord = valuesOrd.get(i);
            if (ord == Long.MIN_VALUE) {
                values.set(i, null);
            } else if (ord < 0) {
                // this wasn't set in last leafreader, so use previous value for lookup
                assert values.get(i) != null;
            } else {
                BytesRef bytesRef = BytesRef.deepCopyOf(lookup.lookupOrd(ord));
                breakerConsumer.accept(bytesRef.bytes.length);
                values.set(i, bytesRef);
                assert values.get(i) != null;
            }
            if (values.get(i) == null) {
                valuesOrd.set(i, Long.MIN_VALUE);
            } else {
                valuesOrd.set(i, newMapping.lookupTerm(values.get(i)));
            }
        }

        lastLookupOrd = null;
        lastLookupValue = null;
        lookup = newMapping;
    }

    @Override
    public boolean requiresRehashingWhenSwitchingLeafReaders() {
        return true;
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
        Releasables.close(valuesOrd, values);
    }
}
