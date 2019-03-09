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

package org.elasticsearch.search.aggregations.bucket.composite;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.lease.Releasables;
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
    private SortedSetDocValues lookup;
    private long currentValue;
    private Long afterValueGlobalOrd;
    private boolean isTopValueInsertionPoint;

    private long lastLookupOrd = -1;
    private BytesRef lastLookupValue;

    GlobalOrdinalValuesSource(BigArrays bigArrays, MappedFieldType type,
                              CheckedFunction<LeafReaderContext, SortedSetDocValues, IOException> docValuesFunc,
                              DocValueFormat format, boolean missingBucket, int size, int reverseMul) {
        super(bigArrays, format, type, missingBucket, size, reverseMul);
        this.docValuesFunc = docValuesFunc;
        this.values = bigArrays.newLongArray(Math.min(size, 100), false);
    }

    @Override
    void copyCurrent(int slot) {
        values = bigArrays.grow(values, slot+1);
        values.set(slot, currentValue);
    }

    @Override
    int compare(int from, int to) {
        return Long.compare(values.get(from), values.get(to)) * reverseMul;
    }

    @Override
    int compareCurrent(int slot) {
        return Long.compare(currentValue, values.get(slot)) * reverseMul;
    }

    @Override
    int compareCurrentWithAfter() {
        int cmp = Long.compare(currentValue, afterValueGlobalOrd);
        if (cmp == 0 && isTopValueInsertionPoint) {
            // the top value is missing in this shard, the comparison is against
            // the insertion point of the top value so equality means that the value
            // is "after" the insertion point.
            return reverseMul;
        }
        return cmp * reverseMul;
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
    void setAfter(Comparable value) {
        if (missingBucket && value == null) {
            afterValue = null;
            afterValueGlobalOrd = -1L;
        } else if (value.getClass() == String.class) {
            afterValue = format.parseBytesRef(value.toString());
        } else {
            throw new IllegalArgumentException("invalid value, expected string, got " + value.getClass().getSimpleName());
        }
    }

    @Override
    BytesRef toComparable(int slot) throws IOException {
        long globalOrd = values.get(slot);
        if (missingBucket && globalOrd == -1) {
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
                    currentValue = -1;
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
