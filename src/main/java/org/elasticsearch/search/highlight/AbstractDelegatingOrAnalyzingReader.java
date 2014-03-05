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

package org.elasticsearch.search.highlight;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.index.*;
import org.apache.lucene.util.*;
import org.apache.lucene.util.XIntBlockPool.SliceReader;
import org.apache.lucene.util.XIntBlockPool.SliceWriter;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.core.StringFieldMapper;
import org.elasticsearch.search.aggregations.bucket.BytesRefHash;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.*;

/**
 * This reader is a pile of hacks designed to allow on the fly re-analyzing for
 * fields for highlighters that need extra data.  It has to be extended to
 * be useful but should work for the FHV and Postings highlighter.
 */
public abstract class AbstractDelegatingOrAnalyzingReader extends FilterAtomicReader implements Releasable {
    private final List<Releasable> releasables = new ArrayList<Releasable>();
    private final SearchContext searchContext;
    private final FetchSubPhase.HitContext hitContext;
    private final boolean forceSource;
    private final TermSetSource termSetSource;

    /**
     * Build one.
     * @param searchContext used to lookup mappers
     * @param hitContext used to find a reader and to load the field values
     * @param forceSource when loading the field values should we force a load from source?
     * @param termSetSource set of valid terms to analyze
     */
    public AbstractDelegatingOrAnalyzingReader(SearchContext searchContext, FetchSubPhase.HitContext hitContext, boolean forceSource,
            TermSetSource termSetSource) {
        // Delegate to a low level reader containing the document.
        super(hitContext.reader());
        this.searchContext = searchContext;
        this.hitContext = hitContext;
        this.forceSource = forceSource;
        this.termSetSource = termSetSource;
    }

    /**
     * Load the field values.
     */
    public List<Object> getValues(FieldMapper<?> mapper) throws IOException {
        if (mapper == null) {
            // No mapper means the field doesn't exist so there isn't anything
            // to fetch.
            return Collections.emptyList();
        }
        return HighlightUtils.loadFieldValues(mapper, searchContext, hitContext, forceSource);
    }

    /**
     * Analyze the contents of a field into a Terms instance.
     * @param field field to analyze
     * @return a terms instance usable by the FVH
     */
    Terms analyzeField(String field) throws IOException {
        FieldMapper<?> mapper = getMapperForField(field);
        List<Object> values = getValues(mapper);
        if (values.isEmpty()) {
            // No values means there can't be term vectors either.
            return null;
        }
        Analyzer analyzer = mapper.indexAnalyzer();
        if (analyzer == null) {
            analyzer = searchContext.analysisService().defaultIndexAnalyzer();
        }
        int positionOffsetGap = 0;
        if (mapper instanceof StringFieldMapper) {
            positionOffsetGap = ((StringFieldMapper)mapper).getPositionOffsetGap();
        }
        Set<String> termSet = termSetSource.termSet(field);
        AnalyzedTerms terms = new AnalyzedTerms(searchContext.pageCacheRecycler(), termSet.size());
        releasables.add(terms); // Make sure to register the terms before the analysis in case it fails
        terms.analyze(field, analyzer, positionOffsetGap, values, termSet);
        return new AnalyzedTermsTermVector(terms);
    }

    private FieldMapper<?> getMapperForField(String field) {
        return HighlightPhase.getMapperForField(field, searchContext, hitContext);
    }

    @Override
    public boolean release() {
        Releasables.release(releasables);
        return true;
    }

    /**
     * Really hacky Fields implementation only really safe for the FHV.
     */
    protected class AnalyzingFields extends Fields {
        @Override
        public Terms terms(String field) throws IOException {
            return analyzeField(field);
        }

        @Override
        public Iterator<String> iterator() {
            throw new IllegalStateException();
        }

        @Override
        public int size() {
            throw new IllegalStateException();
        }
    }

    /**
     * Hacky Fields implementation that delegates to stored term vectors if they
     * exist, otherwise reanalyzes the field on the fly.  Less hacky then
     * AnalyzingFields but still not safe.
     */
    protected class DelegatingOrAnalyzingFields extends FilterFields {
        public DelegatingOrAnalyzingFields(Fields in) {
            super(in);
        }

        @Override
        public Terms terms(String field) throws IOException {
            // This call is very low cost even if there aren't term vectors in
            // the field.
            Terms real = super.terms(field);
            if (real == null || !(real.hasOffsets() && real.hasPositions())) {
                return analyzeField(field);
            }
            return real;
        }
    }

    /**
     * Store for position and offset data that works very similarly to the
     * MemoryIndex but skips a great deal of things not required here like multi
     * term support and sorting the terms.  Also supports limiting terms to
     * a set.
     */
    private static class AnalyzedTerms implements Releasable {
        private final BytesRefHash terms;
        private final XIntBlockPool postings;
        /**
         * Term frequency and start and end offset into the postings.
         */
        private final IntArray extra;

        public AnalyzedTerms(PageCacheRecycler recycler, long capacity) {
            terms = new BytesRefHash(capacity, recycler);
            postings = new XIntBlockPool(recycler);
            extra = BigArrays.newIntArray(terms.capacity() * 3, recycler, false);
        }

        public void analyze(String field, Analyzer analyzer, int positionOffsetGap, List<Object> values, Set<String> termSet)
                throws IOException {
            SliceWriter postingsWriter = new SliceWriter(postings);
            int position = -1;
            int offsetBase = 0;
            for (Object value : values) {
                String valueString = value.toString();
                TokenStream stream = analyzer.tokenStream(field, valueString);
                try {
                    CharTermAttribute charTermAtt = stream.getAttribute(CharTermAttribute.class);
                    PositionIncrementAttribute posIncrAttribute = stream.addAttribute(PositionIncrementAttribute.class);
                    OffsetAttribute offsetAtt = stream.addAttribute(OffsetAttribute.class);
                    stream.reset();
                    while (stream.incrementToken()) {
                        position += posIncrAttribute.getPositionIncrement();
                        if (termSet != null && !termSet.contains(charTermAtt.toString())) {
                            continue;
                        }

                        // Queue the right place to write the posting
                        long ord = terms.add(new BytesRef(charTermAtt));
                        long extraIndex;
                        if (ord < 0) {
                            // Term already exists so read the location of the postings from the header
                            ord = (-ord) - 1;
                            extraIndex = ord * 3;
                            // Note that resetting the reader is very low cost so it isn't a big deal to do it even
                            // if the ord hasn't changed.
                            postingsWriter.reset(extra.get(extraIndex + 2));
                        } else {
                            extraIndex = ord * 3;
                            // Term doesn't exist so start a new slice for it
                            extra.set(extraIndex, 0);
                            extra.set(extraIndex + 1, postingsWriter.startNewSlice());
                        }
                        
                        // Now write the posting
                        postingsWriter.writeInt(position);
                        postingsWriter.writeInt(offsetBase + offsetAtt.startOffset());
                        postingsWriter.writeInt(offsetBase + offsetAtt.endOffset());
                        
                        // Now update the location of the last posting and keep track of the term frequency
                        extra.increment(extraIndex, 1);
                        extra.set(extraIndex + 2, postingsWriter.getCurrentOffset());
                    }
                    stream.end();
                } finally {
                    stream.close();
                }
                position += positionOffsetGap;
                // One space to account for the offset ending at the last character rather than the one beyond it
                // Another to account for space between fields
                offsetBase += valueString.length() + 1;
            }
        }

        @Override
        public boolean release() throws ElasticsearchException {
            postings.release();
            terms.release();
            extra.release();
            return true;
        }
    }
    
    /**
     * Filthy lying implementation of Terms that exposes AnalyzedTerms in a way
     * that the FHV can handle.
     */
    private static class AnalyzedTermsTermVector extends Terms {
        private final AnalyzedTerms terms;
        
        public AnalyzedTermsTermVector(AnalyzedTerms terms) {
            this.terms = terms;
        }

        @Override
        public TermsEnum iterator(TermsEnum reuse) throws IOException {
            return new AnalyzedTermsEnum(terms);
        }

        @Override
        public boolean hasFreqs() {
            return true;
        }

        @Override
        public boolean hasOffsets() {
            return true;
        }

        @Override
        public boolean hasPositions() {
            return true;
        }

        @Override
        public boolean hasPayloads() {
            return false;
        }
        
        @Override
        public Comparator<BytesRef> getComparator() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long size() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getSumTotalTermFreq() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getSumDocFreq() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getDocCount() throws IOException {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Terms enum that doesn't spit out the terms in any particular order.  Horrible
     * for anything but the FVH or PostingsHighlighter.
     */
    private static class AnalyzedTermsEnum extends TermsEnum {
        private final BytesRef ref = new BytesRef();
        private final AnalyzedTerms terms;
        private long current = -1;
        
        public AnalyzedTermsEnum(AnalyzedTerms terms) {
            this.terms = terms;
        }

        /**
         * The FVH expects to iterate over all the terms.
         */
        @Override
        public BytesRef next() throws IOException {
            current++;
            if (current >= terms.terms.size()) {
                return null;
            }
            terms.terms.get(current, ref);
            return ref;
        }

        @Override
        public BytesRef term() throws IOException {
            return ref;
        }

        /**
         * The Postings highlighter expects to be able to seek to the term.
         */
        @Override
        public boolean seekExact(BytesRef text) throws IOException {
            current = terms.terms.find(text);
            return current >= 0;
        }

        @Override
        public DocsAndPositionsEnum docsAndPositions(Bits liveDocs, DocsAndPositionsEnum reuse, int flags) throws IOException {
            return new AnalyzedTermsDocsAndPositionsEnum(terms, current);
        }

        @Override
        public Comparator<BytesRef> getComparator() {
            throw new UnsupportedOperationException();
        }

        @Override
        public SeekStatus seekCeil(BytesRef text) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void seekExact(long ord) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long ord() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long totalTermFreq() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public DocsEnum docs(Bits liveDocs, DocsEnum reuse, int flags) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int docFreq() throws IOException {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * DocsAndPositionsEnum that reads from AnalyzedTerms.
     */
    private static class AnalyzedTermsDocsAndPositionsEnum extends DocsAndPositionsEnum {
        private final SliceReader postingsReader;
        private final int freq;
        private int startOffset;
        private int endOffset;
        
        public AnalyzedTermsDocsAndPositionsEnum(AnalyzedTerms terms, long currentTerm) {
            postingsReader = new SliceReader(terms.postings);
            long extraIndex = currentTerm * 3;
            postingsReader.reset(terms.extra.get(extraIndex + 1), terms.extra.get(extraIndex + 2));
            freq = terms.extra.get(extraIndex);
        }

        @Override
        public int nextPosition() throws IOException {
            int position = postingsReader.readInt();
            startOffset = postingsReader.readInt();
            endOffset = postingsReader.readInt();
            return position;
        }

        @Override
        public int startOffset() throws IOException {
            return startOffset;
        }

        @Override
        public int endOffset() throws IOException {
            return endOffset;
        }

        @Override
        public int freq() throws IOException {
            return freq;
        }

        @Override
        public int nextDoc() throws IOException {
            return 0;
        }

        @Override
        public long cost() {
            return 0;
        }

        /**
         * The Postings highlighter advances to get the doc it is looking for but we only have one so
         * we just tell it we're already there.
         */
        @Override
        public int advance(int target) throws IOException {
           return target;
        }

        /**
         * The Postings highlighter tries to check if we're already on the doc that it is looking for.
         * Since advance is free we just tell it we're on doc 0 and it'll advance or not.  Either way
         * we don't care because advance is a noop.
         */
        @Override
        public int docID() {
            return 0;
        }
        
        @Override
        public BytesRef getPayload() throws IOException {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Called to fetch a set of fields to analyze per term.
     */
    public interface TermSetSource {
        /**
         * Get the terms that should be highlighted for field.
         * @param field field being highlighted
         * @return set of terms to highlight
         */
        Set<String> termSet(String field);
    }
}
