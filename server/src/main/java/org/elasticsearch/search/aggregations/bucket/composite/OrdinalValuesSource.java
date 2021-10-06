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
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    // doc-values lookup, cached by LeafReaderContext ordinal
    private final Map<Integer, SortedSetDocValues> dvsLookup = new HashMap<>();

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

    OrdinalValuesSource(
        BigArrays bigArrays,
        LongConsumer breakerConsumer,
        MappedFieldType type,
        CheckedFunction<LeafReaderContext, SortedSetDocValues, IOException> docValuesFunc,
        DocValueFormat format,
        boolean missingBucket,
        MissingOrder missingOrder,
        int size,
        int reverseMul
    ) {
        super(bigArrays, format, type, missingBucket, missingOrder, size, reverseMul);
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
        return compareInternal(valuesOrd.get(from), valuesOrd.get(to), valuesUnmapped.get(from), valuesUnmapped.get(to));
    }

    @Override
    int compareCurrent(int slot) {
        assert currentValueOrd != null;
        assert slot < numSlots;
        return compareInternal(currentValueOrd, valuesOrd.get(slot), currentValueUnmapped, valuesUnmapped.get(slot));
    }

    @Override
    int compareCurrentWithAfter() {
        assert currentValueOrd != null && afterValueOrd != null;
        return compareInternal(currentValueOrd, afterValueOrd, currentValueUnmapped, afterValue);
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
            return Long.compare(ord1, ord2) * reverseMul;
        } else if (ord1 == Long.MIN_VALUE || ord2 == Long.MIN_VALUE) {
            return Long.compare(ord1, ord2) * missingOrder.compareAnyValueToMissing(reverseMul);
        } else if (ord1 < 0 && ord2 < 0) {
            if (ord1 == ord2) {
                // we need to compare actual terms to properly order
                assert bytesRef1 != null && bytesRef2 != null;
                return bytesRef1.compareTo(bytesRef2) * reverseMul;
            }
            return Long.compare(-ord1 - 1, -ord2 - 1) * reverseMul;
        } else {
            if (ord1 < 0) {
                assert ord1 < 0 && ord2 >= 0;
                int cmp = Long.compare(-ord1 - 1, ord2);
                if (cmp == 0) {
                    return -1 * reverseMul;
                }
                return cmp * reverseMul;
            } else {
                assert ord1 >= 0 && ord2 < 0;
                int cmp = Long.compare(ord1, -ord2 - 1);
                if (cmp == 0) {
                    return reverseMul;
                }
                return cmp * reverseMul;
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
        if (leafReaderContextChanged) {
            // use a separate instance for ordinal and term lookups, that is cached per segment
            // to speed up sorted collections that call getLeafCollector once per term (see above)
            final SortedSetDocValues newLookup = dvsLookup.computeIfAbsent(context.ord, k -> {
                try {
                    return docValuesFunc.apply(context);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
            remapOrdinals(lookup, newLookup);
            lookup = newLookup;
            leafReaderOrd = context.ord;
        }

        // and creates a SortedSetDocValues to iterate over the values
        final SortedSetDocValues it = docValuesFunc.apply(context);
        assert leafReaderContextChanged == false || invariant(); // for performance reasons only check invariant upon change
        return new LeafBucketCollector() {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                // caller of getLeafCollector ensures that collection happens before requesting a new leaf collector
                // this is important as ordinals only make sense in the context of the current lookup
                if (it.advanceExact(doc)) {
                    long ord;
                    while ((ord = it.nextOrd()) != NO_MORE_ORDS) {
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
        if (leafReaderContextChanged) {
            // use a separate instance for ordinal and term lookups, that is cached per segment
            // to speed up sorted collections that call getLeafCollector once per term
            final SortedSetDocValues newLookup = dvsLookup.computeIfAbsent(context.ord, k -> {
                try {
                    return docValuesFunc.apply(context);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
            remapOrdinals(lookup, newLookup);
            lookup = newLookup;
        }
        currentValueOrd = lookup.lookupTerm(term);
        currentValueUnmapped = null;
        leafReaderOrd = context.ord;
        assert currentValueOrd >= 0;
        assert leafReaderContextChanged == false || invariant(); // for performance reasons only check invariant upon change
        return next;
    }

    private static class Slot implements Comparable<Slot> {
        final int index;
        final long ord;
        final BytesRef unmapped;

        private Slot(int index, long ord, BytesRef unmapped) {
            assert ord >= 0 || unmapped != null;
            this.index = index;
            this.ord = ord;
            this.unmapped = unmapped;
        }

        @Override
        public int compareTo(Slot other) {
            if (ord < 0 && ord == other.ord) {
                assert unmapped != null && other.unmapped != null;
                // compare by original term if both ordinals are insertion points (negative value)
                return unmapped.compareTo(other.unmapped);
            }
            long norm1 = ord < 0 ? -ord - 1 : ord;
            long norm2 = other.ord < 0 ? -other.ord - 1 : other.ord;
            int cmp = Long.compare(norm1, norm2);
            return cmp == 0 ? Long.compare(ord, other.ord) : cmp;
        }
    }

    /**
     * Remaps ordinals when switching LeafReaders. It's possible that a term is not mapped for the new LeafReader,
     * in that case remember the term so that future remapping steps can accurately be done.
     */
    private void remapOrdinals(SortedSetDocValues oldMapping, SortedSetDocValues newMapping) throws IOException {
        // speed up the lookups by sorting ordinals first
        List<Slot> sorted = new ArrayList<>();
        for (int i = 0; i < numSlots; i++) {
            long ord = valuesOrd.get(i);
            if (ord != Long.MIN_VALUE) {
                sorted.add(new Slot(i, ord, ord < 0 ? valuesUnmapped.get(i) : null));
            }
        }
        Collections.sort(sorted);

        long lastOldOrd = Long.MIN_VALUE;
        long lastNewOrd = Long.MIN_VALUE;
        BytesRef lastUnmapped = null;
        for (Slot slot : sorted) {
            final long index = slot.index;
            final long oldOrd = slot.ord;
            final BytesRef unmapped = slot.unmapped;
            final long newOrd;
            if (oldOrd >= 0) {
                if (lastOldOrd == oldOrd) {
                    newOrd = lastNewOrd;
                    if (newOrd < 0) {
                        setValueWithBreaking(index, lastUnmapped);
                    }
                } else {
                    final BytesRef newVal = oldMapping.lookupOrd(oldOrd);
                    newOrd = newMapping.lookupTerm(newVal);
                    if (newOrd < 0) {
                        setValueWithBreaking(index, BytesRef.deepCopyOf(newVal));
                    }
                }
            } else {
                // the original term is missing in the dictionary
                assert unmapped != null;
                newOrd = newMapping.lookupTerm(unmapped);
                if (newOrd >= 0) {
                    setValueWithBreaking(index, null);
                }
            }
            lastOldOrd = oldOrd;
            lastNewOrd = newOrd;
            lastUnmapped = valuesUnmapped.get(index);
            valuesOrd.set(index, newOrd);
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
        if (checkIfSortedDocsIsApplicable(reader, fieldType) == false
            || fieldType instanceof StringFieldType == false
            || (query != null && query.getClass() != MatchAllDocsQuery.class)) {
            return null;
        }
        return new TermsSortedDocsProducer(fieldType.name());
    }

    @Override
    public void close() {
        Releasables.close(valuesOrd, valuesUnmapped);
    }
}
