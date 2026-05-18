/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.lucene.index;

import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FilteredDocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOBooleanSupplier;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A frequency TermsEnum that returns frequencies derived from a collection of
 * cached leaf termEnums. It also allows to provide a filter to explicitly
 * compute frequencies only for docs that match the filter (heavier!).
 */
public class FilterableTermsEnum extends TermsEnum {

    static class Holder {
        final TermsEnum termsEnum;
        @Nullable
        PostingsEnum docsEnum;
        @Nullable
        final Bits bits;

        Holder(TermsEnum termsEnum, Bits bits) {
            this.termsEnum = termsEnum;
            this.bits = bits;
        }
    }

    static final String UNSUPPORTED_MESSAGE =
        "This TermsEnum only supports #seekExact(BytesRef) as well as #docFreq() and #totalTermFreq()";
    protected static final int NOT_FOUND = -1;
    private final Holder[] enums;
    protected int currentDocFreq = 0;
    protected long currentTotalTermFreq = 0;
    protected BytesRef current;
    protected final int docsEnumFlag;

    public FilterableTermsEnum(IndexReader reader, String field, int docsEnumFlag, @Nullable Query filter) throws IOException {
        if ((docsEnumFlag != PostingsEnum.FREQS) && (docsEnumFlag != PostingsEnum.NONE)) {
            throw new IllegalArgumentException("invalid docsEnumFlag of " + docsEnumFlag);
        }
        this.docsEnumFlag = docsEnumFlag;
        List<LeafReaderContext> leaves = reader.leaves();
        List<Holder> enums = new ArrayList<>(leaves.size());
        final Weight weight;
        if (filter == null) {
            weight = null;
        } else {
            final IndexSearcher searcher = new IndexSearcher(reader);
            searcher.setQueryCache(null);
            weight = searcher.createWeight(searcher.rewrite(filter), ScoreMode.COMPLETE_NO_SCORES, 1f);
        }
        for (LeafReaderContext context : leaves) {
            Terms terms = context.reader().terms(field);
            if (terms == null) {
                continue;
            }
            TermsEnum termsEnum = terms.iterator();
            if (termsEnum == null) {
                continue;
            }
            BitSet bits = null;
            if (weight != null) {
                Scorer scorer = weight.scorer(context);
                if (scorer == null) {
                    // fully filtered, none matching, no need to iterate on this
                    continue;
                }
                DocIdSetIterator docs = scorer.iterator();

                // we want to force apply deleted docs
                final Bits liveDocs = context.reader().getLiveDocs();
                if (liveDocs != null) {
                    docs = new FilteredDocIdSetIterator(docs) {
                        @Override
                        protected boolean match(int doc) {
                            return liveDocs.get(doc);
                        }
                    };
                }

                bits = BitSet.of(docs, context.reader().maxDoc());
            }
            enums.add(new Holder(termsEnum, bits));
        }
        this.enums = enums.toArray(new Holder[enums.size()]);
    }

    @Override
    public BytesRef term() throws IOException {
        return current;
    }

    @Override
    public AttributeSource attributes() {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public boolean seekExact(BytesRef text) throws IOException {
        int docFreq = 0;
        long totalTermFreq = 0;
        for (Holder anEnum : enums) {
            if (anEnum.termsEnum.seekExact(text)) {
                if (anEnum.bits == null) {
                    docFreq += anEnum.termsEnum.docFreq();
                    if (docsEnumFlag == PostingsEnum.FREQS) {
                        long leafTotalTermFreq = anEnum.termsEnum.totalTermFreq();
                        if (totalTermFreq == -1 || leafTotalTermFreq == -1) {
                            totalTermFreq = -1;
                            continue;
                        }
                        totalTermFreq += leafTotalTermFreq;
                    }
                } else {
                    final PostingsEnum docsEnum = anEnum.docsEnum = anEnum.termsEnum.postings(anEnum.docsEnum, docsEnumFlag);
                    // 2 choices for performing same heavy loop - one attempts to calculate totalTermFreq and other does not
                    if (docsEnumFlag == PostingsEnum.FREQS) {
                        for (int docId = docsEnum.nextDoc(); docId != DocIdSetIterator.NO_MORE_DOCS; docId = docsEnum.nextDoc()) {
                            if (anEnum.bits != null && anEnum.bits.get(docId) == false) {
                                continue;
                            }
                            docFreq++;
                            // docsEnum.freq() returns 1 if doc indexed with IndexOptions.DOCS_ONLY so no way of knowing if value
                            // is really 1 or unrecorded when filtering like this
                            totalTermFreq += docsEnum.freq();
                        }
                    } else {
                        for (int docId = docsEnum.nextDoc(); docId != DocIdSetIterator.NO_MORE_DOCS; docId = docsEnum.nextDoc()) {
                            if (anEnum.bits != null && anEnum.bits.get(docId) == false) {
                                continue;
                            }
                            // docsEnum.freq() behaviour is undefined if docsEnumFlag==PostingsEnum.FLAG_NONE so don't bother with call
                            docFreq++;
                        }
                    }
                }
            }
        }
        if (docFreq > 0) {
            currentDocFreq = docFreq;
            currentTotalTermFreq = totalTermFreq;
            current = text;
            return true;
        } else {
            currentDocFreq = NOT_FOUND;
            currentTotalTermFreq = NOT_FOUND;
            current = null;
            return false;
        }
    }

    @Override
    public IOBooleanSupplier prepareSeekExact(BytesRef bytesRef) {
        return () -> this.seekExact(bytesRef);
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
    public void seekExact(long ord) throws IOException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public void seekExact(BytesRef term, TermState state) throws IOException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public TermState termState() throws IOException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public SeekStatus seekCeil(BytesRef text) throws IOException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public long ord() throws IOException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public ImpactsEnum impacts(int flags) throws IOException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public BytesRef next() throws IOException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }
}
