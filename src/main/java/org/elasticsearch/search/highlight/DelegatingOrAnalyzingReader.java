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

import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FilterAtomicReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.memory.MemoryIndex;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * This reader is a pile of hacks designed to allow on the fly re-analyzing for documents for highlighters that
 * aren't the plain highlighter.
 */
public class DelegatingOrAnalyzingReader extends FilterAtomicReader {
    private final SearchContext searchContext;
    private final FetchSubPhase.HitContext hitContext;
    private final boolean forceSource;

    public DelegatingOrAnalyzingReader(SearchContext searchContext, FetchSubPhase.HitContext hitContext,
            boolean forceSource) {
        // Delegate to a low level reader containing the document.
        super(hitContext.reader());
        this.searchContext = searchContext;
        this.hitContext = hitContext;
        this.forceSource = forceSource;
    }
    
    public AtomicReader getDelegate() {
        return in;
    }

    @Override
    public Fields getTermVectors(int docId) throws IOException {
        // TODO I wonder if we can push this call until we have the field so we can make an educated guess
        // (using the mapper) as to whether there might be term vectors for the field.
        Fields real = super.getTermVectors(docId);
        if (real == null) {
            return new AnalyzingFields(docId);
        }
        return new DelegatingOrAnalyzingFields(real, docId);
    }

    private Terms analyzeField(int docId, String field) throws IOException {
        // Note that we have to look the mapper up again because we might not be analyzing the field for
        // which the highlighter already has the mapper.
        FieldMapper<?> mapper = HighlightPhase.getMapperForField(field, searchContext, hitContext);
        if (mapper == null) {
            // No mapper means the field doesn't exist so there can't be term vectors.
            return null;
        }
        List<Object> values = HighlightUtils.loadFieldValues(mapper, searchContext, hitContext, forceSource);
        if (values.isEmpty()) {
            // No values means there can't be term vectors either.
            return null;
        }
        Analyzer analyzer = mapper.indexAnalyzer();
        if (analyzer == null) {
            analyzer = searchContext.analysisService().defaultIndexAnalyzer();
        }
        // TODO switch this with a scheme that lets us limit to fewer terms or cache it or, something?
        MemoryIndex index = new MemoryIndex(true);
        // Hack to properly handle multi valued fields.... need to do TODO above.
        String value = Joiner.on(' ').join(Iterables.transform(values, Functions.toStringFunction()));
        index.addField(field, value.toString(), analyzer);
        // Note that we always have to use docId 0 with the MemoryIndex because we're only ever sending it one doc.
        Terms t = index.createSearcher().getIndexReader().getTermVector(0, field);
        assert t != null: "If this is null them something is wrong with this method.";
        return t;
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
            // This call is very low cost even if there aren't term vectors in the field.
            Terms real = super.terms(field);
            if (real == null) {
                return analyzeField(docId, field);
            }
            return real;
        }
    }
}
