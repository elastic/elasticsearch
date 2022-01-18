/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.lucene.index;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.io.IOException;

/**
 * A frequency terms enum that maintains a cache of docFreq, totalTermFreq, or both for repeated term lookup.
 */
public class FreqTermsEnum extends FilterableTermsEnum implements Releasable {

    private static final int INITIAL_NUM_TERM_FREQS_CACHED = 512;
    private final BigArrays bigArrays;
    private IntArray termDocFreqs;
    private LongArray termsTotalFreqs;
    private BytesRefHash cachedTermOrds;
    private final boolean needDocFreqs;
    private final boolean needTotalTermFreqs;

    public FreqTermsEnum(
        IndexReader reader,
        String field,
        boolean needDocFreq,
        boolean needTotalTermFreq,
        @Nullable Query filter,
        BigArrays bigArrays
    ) throws IOException {
        super(reader, field, needTotalTermFreq ? PostingsEnum.FREQS : PostingsEnum.NONE, filter);
        this.bigArrays = bigArrays;
        this.needDocFreqs = needDocFreq;
        this.needTotalTermFreqs = needTotalTermFreq;
        if (needDocFreq) {
            termDocFreqs = bigArrays.newIntArray(INITIAL_NUM_TERM_FREQS_CACHED, false);
        } else {
            termDocFreqs = null;
        }
        if (needTotalTermFreq) {
            termsTotalFreqs = bigArrays.newLongArray(INITIAL_NUM_TERM_FREQS_CACHED, false);
        } else {
            termsTotalFreqs = null;
        }
        cachedTermOrds = new BytesRefHash(INITIAL_NUM_TERM_FREQS_CACHED, bigArrays);
    }

    @Override
    public boolean seekExact(BytesRef text) throws IOException {
        // Check cache
        long currentTermOrd = cachedTermOrds.add(text);
        if (currentTermOrd < 0) { // already seen, initialize instance data with the cached frequencies
            currentTermOrd = -1 - currentTermOrd;
            boolean found = true;
            if (needDocFreqs) {
                currentDocFreq = termDocFreqs.get(currentTermOrd);
                found = currentDocFreq != NOT_FOUND;
            }
            if (needTotalTermFreqs) {
                currentTotalTermFreq = termsTotalFreqs.get(currentTermOrd);
                found = currentTotalTermFreq != NOT_FOUND;
            }
            current = found ? text : null;
            return found;
        }

        // Cache miss - gather stats
        final boolean found = super.seekExact(text);

        // Cache the result - found or not.
        if (needDocFreqs) {
            termDocFreqs = bigArrays.grow(termDocFreqs, currentTermOrd + 1);
            termDocFreqs.set(currentTermOrd, currentDocFreq);
        }
        if (needTotalTermFreqs) {
            termsTotalFreqs = bigArrays.grow(termsTotalFreqs, currentTermOrd + 1);
            termsTotalFreqs.set(currentTermOrd, currentTotalTermFreq);
        }
        return found;
    }

    @Override
    public void close() {
        try {
            Releasables.close(cachedTermOrds, termDocFreqs, termsTotalFreqs);
        } finally {
            cachedTermOrds = null;
            termDocFreqs = null;
            termsTotalFreqs = null;
        }
    }

}
