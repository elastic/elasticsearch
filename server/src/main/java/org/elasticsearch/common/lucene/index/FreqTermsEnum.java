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

package org.elasticsearch.common.lucene.index;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.common.util.LongArray;

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


    public FreqTermsEnum(IndexReader reader, String field, boolean needDocFreq, boolean needTotalTermFreq,
            @Nullable Query filter, BigArrays bigArrays) throws IOException {
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
        //Check cache
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
        
        //Cache miss - gather stats
        final boolean found = super.seekExact(text);

        //Cache the result - found or not. 
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
