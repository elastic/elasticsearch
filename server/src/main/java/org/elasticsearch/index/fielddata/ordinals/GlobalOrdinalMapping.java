/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata.ordinals;

import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;

import java.io.IOException;

/**
 * A {@link SortedSetDocValues} implementation that returns global ordinals
 * instead of segment ordinals.
 */
final class GlobalOrdinalMapping extends SortedSetDocValues {

    private final SortedSetDocValues values;
    private final OrdinalMap ordinalMap;
    private final LongValues mapping;
    private final TermsEnum[] lookups;

    GlobalOrdinalMapping(OrdinalMap ordinalMap, SortedSetDocValues values, TermsEnum[] lookups, int segmentIndex) {
        super();
        this.values = values;
        this.lookups = lookups;
        this.ordinalMap = ordinalMap;
        this.mapping = ordinalMap.getGlobalOrds(segmentIndex);
    }

    @Override
    public long getValueCount() {
        return ordinalMap.getValueCount();
    }

    public long getGlobalOrd(long segmentOrd) {
        return mapping.get(segmentOrd);
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        return values.advanceExact(target);
    }

    @Override
    public long nextOrd() throws IOException {
        long segmentOrd = values.nextOrd();
        if (segmentOrd == SortedSetDocValues.NO_MORE_ORDS) {
            return SortedSetDocValues.NO_MORE_ORDS;
        } else {
            return getGlobalOrd(segmentOrd);
        }
    }

    @Override
    public int docValueCount() {
        return values.docValueCount();
    }

    @Override
    public BytesRef lookupOrd(long globalOrd) throws IOException {
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
