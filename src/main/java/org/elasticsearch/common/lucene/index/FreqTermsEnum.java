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

import com.google.common.collect.Lists;
import org.apache.lucene.index.*;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.docset.DocIdSets;
import org.elasticsearch.common.lucene.search.ApplyAcceptedDocsFilter;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.common.util.LongArray;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;

/**
 * A frequency terms enum that maintains a cache of docFreq, totalTermFreq, or both for repeated term lookup. It also
 * allows to provide a filter to explicitly compute frequencies only for docs that match the filter (heavier!).
 */
public class FreqTermsEnum extends TermsEnum implements Releasable {

    static class Holder {
        final TermsEnum termsEnum;
        @Nullable
        DocsEnum docsEnum;
        @Nullable
        final Bits bits;

        Holder(TermsEnum termsEnum, Bits bits) {
            this.termsEnum = termsEnum;
            this.bits = bits;
        }
    }

    private final static int NOT_FOUND = -2;

    private static final int INITIAL_NUM_TERM_FREQS_CACHED = 512;

    private final boolean docFreq;
    private final boolean totalTermFreq;
    private final Holder[] enums;

    private final BigArrays bigArrays;
    private IntArray termDocFreqs;
    private LongArray termsTotalFreqs;
    private BytesRefHash cachedTermOrds;

    private int currentDocFreq = 0;
    private long currentTotalTermFreq = 0;

    private BytesRef current;

    public FreqTermsEnum(IndexReader reader, String field, boolean docFreq, boolean totalTermFreq, @Nullable Filter filter, BigArrays bigArrays) throws IOException {
        this.docFreq = docFreq;
        this.totalTermFreq = totalTermFreq;
        if (!docFreq && !totalTermFreq) {
            throw new ElasticsearchIllegalArgumentException("either docFreq or totalTermFreq must be true");
        }
        List<AtomicReaderContext> leaves = reader.leaves();
        List<Holder> enums = Lists.newArrayListWithExpectedSize(leaves.size());
        for (AtomicReaderContext context : leaves) {
            Terms terms = context.reader().terms(field);
            if (terms == null) {
                continue;
            }
            TermsEnum termsEnum = terms.iterator(null);
            if (termsEnum == null) {
                continue;
            }
            Bits bits = null;
            if (filter != null) {
                if (filter == Queries.MATCH_ALL_FILTER) {
                    bits = context.reader().getLiveDocs();
                } else {
                    // we want to force apply deleted docs
                    filter = new ApplyAcceptedDocsFilter(filter);
                    DocIdSet docIdSet = filter.getDocIdSet(context, context.reader().getLiveDocs());
                    if (DocIdSets.isEmpty(docIdSet)) {
                        // fully filtered, none matching, no need to iterate on this
                        continue;
                    }
                    bits = DocIdSets.toSafeBits(context.reader(), docIdSet);
                }
            }
            enums.add(new Holder(termsEnum, bits));
        }
        this.bigArrays = bigArrays;

        this.enums = enums.toArray(new Holder[enums.size()]);

        if (docFreq) {
            termDocFreqs = bigArrays.newIntArray(INITIAL_NUM_TERM_FREQS_CACHED, false);
        } else {
            termDocFreqs = null;
        }
        if (totalTermFreq) {
            termsTotalFreqs = bigArrays.newLongArray(INITIAL_NUM_TERM_FREQS_CACHED, false);
        } else {
            termsTotalFreqs = null;
        }
        cachedTermOrds = new BytesRefHash(INITIAL_NUM_TERM_FREQS_CACHED, bigArrays);
    }

    @Override
    public BytesRef term() throws IOException {
        return current;
    }

    @Override
    public boolean seekExact(BytesRef text) throws IOException {
        long currentTermOrd = cachedTermOrds.add(text);
        if (currentTermOrd < 0) { // already seen, initialize instance data with the cached frequencies
            currentTermOrd = -1 - currentTermOrd;
            boolean found = true;
            if (docFreq) {
                currentDocFreq = termDocFreqs.get(currentTermOrd);
                if (currentDocFreq == NOT_FOUND) {
                    found = false;
                }
            }
            if (totalTermFreq) {
                currentTotalTermFreq = termsTotalFreqs.get(currentTermOrd);
                if (currentTotalTermFreq == NOT_FOUND) {
                    found = false;
                }
            }
            current = found ? text : null;
            return found;
        }

        boolean found = false;
        int docFreq = 0;
        long totalTermFreq = 0;
        for (Holder anEnum : enums) {
            if (!anEnum.termsEnum.seekExact(text)) {
                continue;
            }
            found = true;
            if (anEnum.bits == null) {
                docFreq += anEnum.termsEnum.docFreq();
                if (this.totalTermFreq) {
                    totalTermFreq += anEnum.termsEnum.totalTermFreq();
                }
            } else {
                DocsEnum docsEnum = anEnum.docsEnum = anEnum.termsEnum.docs(anEnum.bits, anEnum.docsEnum, this.totalTermFreq ? DocsEnum.FLAG_FREQS : DocsEnum.FLAG_NONE);
                for (int docId = docsEnum.nextDoc(); docId != DocIdSetIterator.NO_MORE_DOCS; docId = docsEnum.nextDoc()) {
                    docFreq++;
                    if (this.totalTermFreq) {
                        totalTermFreq += docsEnum.freq();
                    }
                }
            }
        }

        current = found ? text : null;
        if (this.docFreq) {
            if (!found) {
                docFreq = NOT_FOUND;
            }
            currentDocFreq = docFreq;
            termDocFreqs = bigArrays.grow(termDocFreqs, currentTermOrd + 1);
            termDocFreqs.set(currentTermOrd, docFreq);
        }
        if (this.totalTermFreq) {
            if (!found) {
                totalTermFreq = NOT_FOUND;
            } else if (totalTermFreq < 0) {
                // no freqs really..., blast
                totalTermFreq = -1;
            }
            currentTotalTermFreq = totalTermFreq;
            termsTotalFreqs = bigArrays.grow(termsTotalFreqs, currentTermOrd + 1);
            termsTotalFreqs.set(currentTermOrd, totalTermFreq);
        }

        return found;
    }

    @Override
    public int docFreq() throws IOException {
        return currentDocFreq;
    }

    @Override
    public long totalTermFreq() throws IOException {
        return currentTotalTermFreq;
    }

    @Override
    public boolean release() throws ElasticsearchException {
        try {
            Releasables.release(cachedTermOrds, termDocFreqs, termsTotalFreqs);
        } finally {
            cachedTermOrds = null;
            termDocFreqs = null;
            termsTotalFreqs = null;
        }
        return true;
    }

    @Override
    public void seekExact(long ord) throws IOException {
        throw new UnsupportedOperationException("freq terms enum");
    }

    @Override
    public SeekStatus seekCeil(BytesRef text) throws IOException {
        throw new UnsupportedOperationException("freq terms enum");
    }

    @Override
    public long ord() throws IOException {
        throw new UnsupportedOperationException("freq terms enum");
    }

    @Override
    public DocsEnum docs(Bits liveDocs, DocsEnum reuse, int flags) throws IOException {
        throw new UnsupportedOperationException("freq terms enum");
    }

    @Override
    public DocsAndPositionsEnum docsAndPositions(Bits liveDocs, DocsAndPositionsEnum reuse, int flags) throws IOException {
        throw new UnsupportedOperationException("freq terms enum");
    }

    @Override
    public BytesRef next() throws IOException {
        throw new UnsupportedOperationException("freq terms enum");
    }

    @Override
    public Comparator<BytesRef> getComparator() {
        throw new UnsupportedOperationException("freq terms enum");
    }
}
