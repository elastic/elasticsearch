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
 *
 * As it only needs to keep track of the top N composite buckets, and N is typically small, we can just use the segment ordinal for
 * comparison when collecting inside a segment and remap ordinals when we go to the next segment instead of using global ordinals.
 *
 * Ordinals are remapped when visiting a new segment (see {@link #remapOrdinals(SortedSetDocValues, SortedSetDocValues)}).
 * As it's possible that a previously mapped term has no corresponding ordinal on the new LeafReader, we also cache the currently unmapped
 * terms so that future remapping steps can be accurately done.
 *
 * The ordinal Long.MIN_VALUE is used to represent the missing bucket.
 *
 * Other negative values for the ordinal mean that the term is not known to the current lookup
 * (see {@link SortedSetDocValues#lookupTerm(BytesRef)}}) and correspond to -insertionPoint-1.
 *
 * See {@link #invariant()} for more details.
 */
class OrdinalValuesSource extends SingleDimensionValuesSource<BytesRef> {
    private final LongConsumer breakerConsumer; // track how much bytes are stored in the values array
    private final CheckedFunction<LeafReaderContext, SortedSetDocValues, IOException> docValuesFunc;

    private SortedSetDocValues lookup; // current ordinals lookup
    private int leafReaderOrd = -1; // current LeafReaderContext ordinal

    private LongArray valuesOrd; // ordinals, which are remapped whenever we visit a new segment
    private ObjectArray<BytesRef> valuesUnmapped;
    private int numSlots = 0; // number of slots in the above arrays that contain any relevant values

    private Long currentValueOrd;
    private BytesRef currentValueUnmapped;

    private Long afterValueOrd; // null if no afterValue is set

    // small cache to avoid repeated lookups in toComparable
    private Long lastLookupOrd; // null if nothing cached
    private BytesRef lastLookupValue;

    OrdinalValuesSource(BigArrays bigArrays, LongConsumer breakerConsumer, MappedFieldType type,
                        CheckedFunction<LeafReaderContext, SortedSetDocValues, IOException> docValuesFunc,
                        DocValueFormat format, boolean missingBucket, int size, int reverseMul) {
        super(bigArrays, format, type, missingBucket, size, reverseMul);
        this.breakerConsumer = breakerConsumer;
        this.docValuesFunc = docValuesFunc;
        this.valuesOrd = bigArrays.newLongArray(Math.min(size, 100), false);
        this.valuesUnmapped = bigArrays.newObjectArray(Math.min(size, 100));
    }

    /**
     * Class invariant that should hold before and after every invocation of public methods on this class.
     */
    private boolean invariant() {
        assert numSlots <= valuesOrd.size() && valuesOrd.size() == valuesOrd.size();
        for (int i = 0; i < numSlots; i++) {
            assert ordAndValueConsistency(valuesOrd.get(i), valuesUnmapped.get(i));
        }
        if (currentValueOrd != null) {
            assert ordAndValueConsistency(currentValueOrd, currentValueUnmapped);
        }
        if (lastLookupOrd != null) {
            assert lastLookupOrd >= 0 && lastLookupValue != null;
        }
        return true;
    }

    private boolean ordAndValueConsistency(long ordinal, BytesRef value) {
        // The ordinal Long.MIN_VALUE is used to represent the missing bucket.
        assert ordinal != Long.MIN_VALUE || missingBucket;
        // value is cached iff ordinal is unmapped and not missing bucket
        assert (ordinal == Long.MIN_VALUE || ordinal >= 0) == (value == null);

        // ordinals and values are consistent with current lookup
        try {
            if (ordinal >= 0) {
                assert lookup.lookupOrd(ordinal) != null;
            }
            if (value != null) {
                assert lookup.lookupTerm(value) == ordinal;
            }
        } catch (IOException e) {
            assert false : e;
        }
        return true;
    }

    @Override
    void copyCurrent(int slot) {
        numSlots = Math.max(numSlots, slot + 1);
        valuesOrd = bigArrays.grow(valuesOrd, numSlots);
        valuesUnmapped = bigArrays.grow(valuesUnmapped, numSlots);

        assert currentValueUnmapped == null;
        valuesOrd.set(slot, currentValueOrd);
        setValueWithBreaking(slot, currentValueUnmapped);
    }

    private void setValueWithBreaking(long index, BytesRef newValue) {
        BytesRef previousValue = valuesUnmapped.get(index);
        long previousSize = previousValue == null ? 0 : previousValue.length;
        long newSize = newValue == null ? 0 : newValue.length;
        long delta = newSize - previousSize;
        if (delta != 0) {
            breakerConsumer.accept(delta);
        }
        valuesUnmapped.set(index, newValue);
    }

    @Override
    int compare(int from, int to) {
        assert from < numSlots && to < numSlots;
        return compareInternal(valuesOrd.get(from), valuesOrd.get(to), valuesUnmapped.get(from), valuesUnmapped.get(to)) * reverseMul;
    }

    @Override
    int compareCurrent(int slot) {
        assert currentValueOrd != null;
        assert slot < numSlots;
        return compareInternal(currentValueOrd, valuesOrd.get(slot), currentValueUnmapped, valuesUnmapped.get(slot)) * reverseMul;
    }

    @Override
    int compareCurrentWithAfter() {
        assert currentValueOrd != null && afterValueOrd != null;
        return compareInternal(currentValueOrd, afterValueOrd, currentValueUnmapped, afterValue) * reverseMul;
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
    void setAfter(Comparable<?> value) {
        assert invariant();
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
        assert invariant();
    }

    @Override
    BytesRef toComparable(int slot) throws IOException {
        assert slot < numSlots;
        long ord = valuesOrd.get(slot);
        if (ord == Long.MIN_VALUE) {
            assert missingBucket;
            return null;
        } else if (ord < 0) {
            return valuesUnmapped.get(slot);
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
        final boolean leafReaderContextChanged = context.ord != leafReaderOrd;
        assert leafReaderContextChanged == false || invariant(); // for performance reasons only check invariant upon change
        final SortedSetDocValues dvs = docValuesFunc.apply(context);
        if (leafReaderContextChanged) {
            remapOrdinals(lookup, dvs);
            leafReaderOrd = context.ord;
        }
        lookup = dvs;
        assert leafReaderContextChanged == false || invariant(); // for performance reasons only check invariant upon change
        return new LeafBucketCollector() {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                // caller of getLeafCollector ensures that collection happens before requesting a new leaf collector
                // this is important as ordinals only make sense in the context of the current lookup
                assert dvs == lookup;
                if (dvs.advanceExact(doc)) {
                    long ord;
                    while ((ord = dvs.nextOrd()) != NO_MORE_ORDS) {
                        currentValueOrd = ord;
                        currentValueUnmapped = null;
                        next.collect(doc, bucket);
                    }
                } else if (missingBucket) {
                    currentValueOrd = Long.MIN_VALUE;
                    currentValueUnmapped = null;
                    next.collect(doc, bucket);
                }
            }
        };
    }

    @Override
    LeafBucketCollector getLeafCollector(Comparable<BytesRef> value, LeafReaderContext context, LeafBucketCollector next)
        throws IOException {
        final boolean leafReaderContextChanged = context.ord != leafReaderOrd;
        assert leafReaderContextChanged == false || invariant(); // for performance reasons only check invariant upon change
        if (value.getClass() != BytesRef.class) {
            throw new IllegalArgumentException("Expected BytesRef, got " + value.getClass());
        }
        BytesRef term = (BytesRef) value;
        final SortedSetDocValues dvs = docValuesFunc.apply(context);
        if (leafReaderContextChanged) {
            remapOrdinals(lookup, dvs);
            leafReaderOrd = context.ord;
        }
        lookup = dvs;
        assert leafReaderContextChanged == false || invariant(); // for performance reasons only check invariant upon change
        return new LeafBucketCollector() {
            boolean currentValueIsSet = false;

            @Override
            public void collect(int doc, long bucket) throws IOException {
                // caller of getLeafCollector ensures that collection happens before requesting a new leaf collector
                // this is important as ordinals only make sense in the context of the current lookup
                assert dvs == lookup;
                if (currentValueIsSet == false) {
                    if (dvs.advanceExact(doc)) {
                        long ord;
                        while ((ord = dvs.nextOrd()) != NO_MORE_ORDS) {
                            if (term.equals(dvs.lookupOrd(ord))) {
                                currentValueIsSet = true;
                                currentValueOrd = ord;
                                currentValueUnmapped = null;
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
        for (int i = 0; i < numSlots; i++) {
            final long oldOrd = valuesOrd.get(i);
            if (oldOrd != Long.MIN_VALUE) {
                final long newOrd;
                if (oldOrd >= 0) {
                    final BytesRef newVal = oldMapping.lookupOrd(oldOrd);
                    newOrd = newMapping.lookupTerm(newVal);
                    if (newOrd < 0) {
                        setValueWithBreaking(i, BytesRef.deepCopyOf(newVal));
                    }
                } else {
                    newOrd = newMapping.lookupTerm(valuesUnmapped.get(i));
                    if (newOrd >= 0) {
                        setValueWithBreaking(i, null);
                    }
                }
                valuesOrd.set(i, newOrd);
            }
        }

        if (currentValueOrd != null) {
            if (currentValueOrd != Long.MIN_VALUE) {
                final long newOrd;
                if (currentValueOrd >= 0) {
                    final BytesRef newVal = oldMapping.lookupOrd(currentValueOrd);
                    newOrd = newMapping.lookupTerm(newVal);
                    if (newOrd < 0) {
                        currentValueUnmapped = BytesRef.deepCopyOf(newVal);
                    }
                } else {
                    newOrd = newMapping.lookupTerm(currentValueUnmapped);
                    if (newOrd >= 0) {
                        currentValueUnmapped = null;
                    }
                }
                currentValueOrd = newOrd;
            }
        }

        if (afterValue != null) {
            afterValueOrd = newMapping.lookupTerm(afterValue);
        }

        lastLookupOrd = null;
        lastLookupValue = null;
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
        Releasables.close(valuesOrd, valuesUnmapped);
    }
}
