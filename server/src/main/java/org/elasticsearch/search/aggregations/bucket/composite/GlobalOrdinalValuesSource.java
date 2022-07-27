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
    public static final long MISSING_VALUE_FLAG = -1L;
    private final CheckedFunction<LeafReaderContext, SortedSetDocValues, IOException> docValuesFunc;
    private LongArray values;
    private SortedSetDocValues lookup;
    private long currentValue;
    private Long afterValueGlobalOrd;
    private boolean isTopValueInsertionPoint;

    private long lastLookupOrd = -1;
    private BytesRef lastLookupValue;

    GlobalOrdinalValuesSource(
        BigArrays bigArrays,
        MappedFieldType type,
        CheckedFunction<LeafReaderContext, SortedSetDocValues, IOException> docValuesFunc,
        DocValueFormat format,
        boolean missingBucket,
        MissingOrder missingOrder,
        int size,
        int reverseMul
    ) {
        super(bigArrays, format, type, missingBucket, missingOrder, size, reverseMul);
        this.docValuesFunc = docValuesFunc;
        this.values = bigArrays.newLongArray(Math.min(size, 100), false);
    }

    @Override
    void copyCurrent(int slot) {
        values = bigArrays.grow(values, slot + 1);
        values.set(slot, currentValue);
    }

    private int compareInternal(long lhs, long rhs) {
        int mul = (lhs == MISSING_VALUE_FLAG || rhs == MISSING_VALUE_FLAG) ? missingOrder.compareAnyValueToMissing(reverseMul) : reverseMul;
        return Long.compare(lhs, rhs) * mul;
    }

    @Override
    int compare(int from, int to) {
        return compareInternal(values.get(from), values.get(to));
    }

    @Override
    int compareCurrent(int slot) {
        return compareInternal(currentValue, values.get(slot));
    }

    @Override
    int compareCurrentWithAfter() {
        int cmp = compareInternal(currentValue, afterValueGlobalOrd);
        if (cmp == 0 && isTopValueInsertionPoint) {
            // the top value is missing in this shard, the comparison is against
            // the insertion point of the top value so equality means that the value
            // is "after" the insertion point.
            return missingOrder.compareAnyValueToMissing(reverseMul);
        }
        return cmp;
    }

    @Override
    int hashCode(int slot) {
        return Long.hashCode(values.get(slot));
    }

    @Override
    int hashCodeCurrent() {
        return Long.hashCode(currentValue);
    }

    @Override
    void setAfter(Comparable<?> value) {
        if (missingBucket && value == null) {
            afterValue = null;
            afterValueGlobalOrd = MISSING_VALUE_FLAG;
        } else if (value.getClass() == String.class || (fieldType == null)) {
            // the value might be not string if this field is missing in this shard but present in other shards
            // and doesn't have a string type
            afterValue = format.parseBytesRef(value);
        } else if (value.getClass() == BytesRef.class) {
            // The value may be a bytes reference (eg an encoded tsid field)
            afterValue = (BytesRef) value;
        } else {
            throw new IllegalArgumentException("invalid value, expected string, got " + value.getClass().getSimpleName());
        }
    }

    @Override
    BytesRef toComparable(int slot) throws IOException {
        long globalOrd = values.get(slot);
        if (missingBucket && globalOrd == MISSING_VALUE_FLAG) {
            return null;
        } else if (globalOrd == lastLookupOrd) {
            return lastLookupValue;
        } else {
            lastLookupOrd = globalOrd;
            lastLookupValue = BytesRef.deepCopyOf(lookup.lookupOrd(values.get(slot)));
            return lastLookupValue;
        }
    }

    @Override
    LeafBucketCollector getLeafCollector(LeafReaderContext context, LeafBucketCollector next) throws IOException {
        final SortedSetDocValues dvs = docValuesFunc.apply(context);
        if (lookup == null) {
            initLookup(dvs);
        }
        return new LeafBucketCollector() {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (dvs.advanceExact(doc)) {
                    long ord;
                    while ((ord = dvs.nextOrd()) != NO_MORE_ORDS) {
                        currentValue = ord;
                        next.collect(doc, bucket);
                    }
                } else if (missingBucket) {
                    currentValue = MISSING_VALUE_FLAG;
                    next.collect(doc, bucket);
                }
            }
        };
    }

    @Override
    LeafBucketCollector getLeafCollector(Comparable<BytesRef> value, LeafReaderContext context, LeafBucketCollector next)
        throws IOException {
        if (value.getClass() != BytesRef.class) {
            throw new IllegalArgumentException("Expected BytesRef, got " + value.getClass());
        }
        BytesRef term = (BytesRef) value;
        final SortedSetDocValues dvs = docValuesFunc.apply(context);
        if (lookup == null) {
            initLookup(dvs);
        }
        return new LeafBucketCollector() {
            boolean currentValueIsSet = false;

            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (currentValueIsSet == false) {
                    if (dvs.advanceExact(doc)) {
                        long ord;
                        while ((ord = dvs.nextOrd()) != NO_MORE_ORDS) {
                            if (term.equals(lookup.lookupOrd(ord))) {
                                currentValueIsSet = true;
                                currentValue = ord;
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
        Releasables.close(values);
    }

    private void initLookup(SortedSetDocValues dvs) throws IOException {
        lookup = dvs;
        if (afterValue != null && afterValueGlobalOrd == null) {
            afterValueGlobalOrd = lookup.lookupTerm(afterValue);
            if (afterValueGlobalOrd < 0) {
                // convert negative insert position
                afterValueGlobalOrd = -afterValueGlobalOrd - 1;
                isTopValueInsertionPoint = true;
            }
        }
    }
}
