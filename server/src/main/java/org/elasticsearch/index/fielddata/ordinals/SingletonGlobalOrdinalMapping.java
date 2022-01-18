/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.fielddata.ordinals;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;

import java.io.IOException;

/**
 * {@link SortedDocValues} implementation that returns global ordinals instead
 * of segment ordinals.
 */
class SingletonGlobalOrdinalMapping extends SortedDocValues {
    /**
     * Build a {@link SortedDocValues singleton doc values} if possible
     * Lots of other code tries to unwrap the singleton with
     * {@link DocValues#unwrapSingleton} and it'll take a fast path if
     * it gets a singleton. This'll return a singleton that can be
     * unwrapped or {@code null} if the {@link OrdinalMap} and
     * {@link SortedSetDocValues} aren't compatible with {@link SortedDocValues}.
     */
    static SortedSetDocValues singletonIfPossible(OrdinalMap ordinalMap, SortedSetDocValues values, TermsEnum[] lookups, int segmentIndex) {
        /*
         * We can manage a singleton if the total value count
         * fits in an `int` *and* the segment ords are a singleton.
         * We need the first one just because SortedDocValues
         * returns `int` instead of `long`
         */
        if (ordinalMap.getValueCount() > Integer.MAX_VALUE) {
            return null;
        }
        SortedDocValues singleton = DocValues.unwrapSingleton(values);
        if (singleton == null) {
            return null;
        }
        return DocValues.singleton(new SingletonGlobalOrdinalMapping(ordinalMap, singleton, lookups, segmentIndex));
    }

    private final SortedDocValues values;
    private final OrdinalMap ordinalMap;
    private final LongValues mapping;
    private final TermsEnum[] lookups;

    private SingletonGlobalOrdinalMapping(OrdinalMap ordinalMap, SortedDocValues values, TermsEnum[] lookups, int segmentIndex) {
        this.values = values;
        this.lookups = lookups;
        this.ordinalMap = ordinalMap;
        this.mapping = ordinalMap.getGlobalOrds(segmentIndex);
    }

    @Override
    public int getValueCount() {
        return (int) ordinalMap.getValueCount();
    }

    @Override
    public int ordValue() throws IOException {
        return (int) mapping.get(values.ordValue());
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        return values.advanceExact(target);
    }

    @Override
    public BytesRef lookupOrd(int globalOrd) throws IOException {
        final long segmentOrd = ordinalMap.getFirstSegmentOrd(globalOrd);
        int readerIndex = ordinalMap.getFirstSegmentNumber(globalOrd);
        lookups[readerIndex].seekExact(segmentOrd);
        return lookups[readerIndex].term();
    }

    @Override
    public int docID() {
        return values.docID();
    }

    @Override
    public int nextDoc() throws IOException {
        return values.nextDoc();
    }

    @Override
    public int advance(int target) throws IOException {
        return values.advance(target);
    }

    @Override
    public long cost() {
        return values.cost();
    }
}
