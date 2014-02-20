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
import org.apache.lucene.util.ByteBlockPool.DirectAllocator;
import org.apache.lucene.util.BytesRefHash.DirectBytesStartArray;
import org.apache.lucene.util.IntBlockPool.SliceReader;
import org.apache.lucene.util.IntBlockPool.SliceWriter;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.core.StringFieldMapper;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * This reader is a pile of hacks designed to allow on the fly re-analyzing for
 * documents for highlighters that aren't the plain highlighter.
 */
public class DelegatingOrAnalyzingReader extends FilterAtomicReader {
    private final SearchContext searchContext;
    private final FetchSubPhase.HitContext hitContext;
    private final boolean forceSource;
    /**
     * Optional source of terms to ananlyze.  If left null then all terms in the field will be analyzed.
     */
    @Nullable
    private final TermSetSource termSetSource;
    private Map<String, List<Object>> valuesCache;

    public DelegatingOrAnalyzingReader(SearchContext searchContext, FetchSubPhase.HitContext hitContext, boolean forceSource,
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
        return getValues(mapper, false);
    }

    @Override
    public Fields getTermVectors(int docId) throws IOException {
        // TODO I wonder if we can push this call until we have the field so we
        // can make an educated guess
        // (using the mapper) as to whether there might be term vectors for the
        // field.
        Fields real = super.getTermVectors(docId);
        if (real == null) {
            return new AnalyzingFields(docId);
        }
        return new DelegatingOrAnalyzingFields(real, docId);
    }

    Terms analyzeField(int docId, String field) throws IOException {
        FieldMapper<?> mapper = getMapperForField(field);
        List<Object> values = getValues(mapper, true);
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
        Set<String> termSet = null;
        if (termSetSource != null) {
            termSet = termSetSource.termSet(field);
        }
        AnalyzedTerms terms = new AnalyzedTerms(field, analyzer, positionOffsetGap, values, termSet);
        return new AnalyzedTermsTermVector(terms);
    }

    private FieldMapper<?> getMapperForField(String field) {
        return HighlightPhase.getMapperForField(field, searchContext, hitContext);
    }

    private List<Object> getValues(FieldMapper<?> mapper, boolean addToCache) throws IOException {
        if (mapper == null) {
            // No mapper means the field doesn't exist so there isn't anything
            // to fetch.
            return Collections.emptyList();
        }
        List<Object> values;
        if (valuesCache != null) {
            // Use the source path as the key so we don't have to load things
            // with the same source twice.
            values = valuesCache.get(mapper.names().sourcePath());
            if (values != null) {
                return values;
            }
        }
        // Will never return null so caches well
        values = HighlightUtils.loadFieldValues(mapper, searchContext, hitContext, forceSource);
        if (addToCache) {
            if (valuesCache == null) {
                valuesCache = new HashMap<String, List<Object>>();
            }
            valuesCache.put(mapper.names().sourcePath(), values);
        }
        return values;
    }

    /**
     * Really hacky Fields implementation mostly useful for the FVH.
     */
    private class AnalyzingFields extends Fields {
        private final int docId;

        public AnalyzingFields(int docId) {
            this.docId = docId;
        }

        @Override
        public Terms terms(String field) throws IOException {
            return analyzeField(docId, field);
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
     * exist, otherwise reanalyzes the field on the fly.
     */
    private class DelegatingOrAnalyzingFields extends FilterFields {
        private final int docId;

        public DelegatingOrAnalyzingFields(Fields in, int docId) {
            super(in);
            this.docId = docId;
        }

        @Override
        public Terms terms(String field) throws IOException {
            // This call is very low cost even if there aren't term vectors in
            // the field.
            Terms real = super.terms(field);
            if (real == null) {
                return analyzeField(docId, field);
            }
            return real;
        }
    }

    /**
     * Store for position and offset data that works very similarly to the
     * MemoryIndex but skips a great deal of things not required here like multi
     * term support.
     */
    private static class AnalyzedTerms {
        private final ExtraArraysByteStartArray extra = new ExtraArraysByteStartArray(BytesRefHash.DEFAULT_CAPACITY);
        private final BytesRefHash terms = new BytesRefHash(new ByteBlockPool(new DirectAllocator()), 
                BytesRefHash.DEFAULT_CAPACITY, extra);
        private final IntBlockPool postings = new IntBlockPool();

        private static long totalTime = 0;
        private static long overheadTime = 0;
        
        static {
            final ScheduledExecutorService e = Executors.newScheduledThreadPool(1);
            final Runnable r = new Runnable() {
                @Override
                public void run() {
                    System.err.println(totalTime + " " + overheadTime);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException();
                    }
                }
            };
            e.scheduleAtFixedRate(r, 5, 5, TimeUnit.SECONDS);
        }
        
        public AnalyzedTerms(String field, Analyzer analyzer, int positionOffsetGap, List<Object> values, Set<String> termSet)
                throws IOException {
            SliceWriter postingsWriter = new SliceWriter(postings);
            
            long start = System.currentTimeMillis();
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
                        long startOverhead = System.currentTimeMillis();
                        position += posIncrAttribute.getPositionIncrement();
                        if (termSet != null && !termSet.contains(charTermAtt.toString())) {
                            continue;
                        }

                        // Queue the right place to write the posting
                        int ord = terms.add(new BytesRef(charTermAtt));
                        if (ord < 0) {
                            // Term already exists so read the location of the postings from the header
                            ord = (-ord) - 1;
                            postingsWriter.reset(extra.end[ord]);
                        } else {
                            // Term doesn't exist so start a new slice for it
                            extra.start[ord] = postingsWriter.startNewSlice();
                        }
                        
                        // Now write the posting
                        postingsWriter.writeInt(position);
                        postingsWriter.writeInt(offsetBase + offsetAtt.startOffset());
                        postingsWriter.writeInt(offsetBase + offsetAtt.endOffset());
                        
                        // Now update the location of the last posting and keep track of the term frequency
                        extra.end[ord] = postingsWriter.getCurrentOffset();
                        extra.freq[ord]++;
                        overheadTime += System.currentTimeMillis() - startOverhead;
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
            totalTime += System.currentTimeMillis() - start;
        }
        
        /**
         * Piggybacks on the array size management logic from DirectBytesStartArray to trigger management of more useful arrays for
         * storing the postings.  This is quite nearly a copy of {@link MemoryIndex$SliceByteStartArray} but it really is the right
         * way to do it.
         */
        private static class ExtraArraysByteStartArray extends DirectBytesStartArray {
            private int[] start;
            private int[] end;
            private int[] freq;
            
            public ExtraArraysByteStartArray(int initSize) {
              super(initSize);
            }
            
            @Override
            public int[] init() {
              final int[] ord = super.init();
              int oversize = ArrayUtil.oversize(ord.length, RamUsageEstimator.NUM_BYTES_INT);
              start = new int[oversize];
              end = new int[oversize];
              freq = new int[oversize];
              assert start.length >= ord.length;
              assert end.length >= ord.length;
              assert freq.length >= ord.length;
              return ord;
            }

            @Override
            public int[] grow() {
              final int[] ord = super.grow();
              if (start.length < ord.length) {
                start = ArrayUtil.grow(start, ord.length);
                end = ArrayUtil.grow(end, ord.length);
                freq = ArrayUtil.grow(freq, ord.length);
              }      
              assert start.length >= ord.length;
              assert end.length >= ord.length;
              assert freq.length >= ord.length;
              return ord;
            }

            @Override
            public int[] clear() {
             start = end = freq = null;
             return super.clear();
            }
            
          }
    }
    
    /**
     * Filthy lying implementation of Terms designed to only work with the FVH.
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
     * Terms enum that doesn't spit out the terms in any particular order.
     */
    private static class AnalyzedTermsEnum extends TermsEnum {
        private final BytesRef ref = new BytesRef();
        private final AnalyzedTerms terms;
        private int current = -1;
        
        public AnalyzedTermsEnum(AnalyzedTerms terms) {
            this.terms = terms;
        }

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
        public int docFreq() throws IOException {
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
    }
    
    private static class AnalyzedTermsDocsAndPositionsEnum extends DocsAndPositionsEnum {
        private final SliceReader postingsReader;
        private final int freq;
        private int startOffset;
        private int endOffset;
        
        public AnalyzedTermsDocsAndPositionsEnum(AnalyzedTerms terms, int currentTerm) {
            postingsReader = new SliceReader(terms.postings);
            postingsReader.reset(terms.extra.start[currentTerm], terms.extra.end[currentTerm]);
            freq = terms.extra.freq[currentTerm];
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

        @Override
        public BytesRef getPayload() throws IOException {
            throw new UnsupportedOperationException();
        }
        
        @Override
        public int advance(int target) throws IOException {
            throw new UnsupportedOperationException();
        }
        
        @Override
        public int docID() {
            throw new UnsupportedOperationException();
        }
    }

    public interface TermSetSource {
        /**
         * Get the terms that should be highlighted for field.
         * @param field field being highlighted
         * @return set of terms to highlight
         */
        Set<String> termSet(String field);
    }
}
