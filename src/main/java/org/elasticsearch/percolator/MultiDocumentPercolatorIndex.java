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


package org.elasticsearch.percolator;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.*;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;

import java.io.IOException;



class MultiDocumentPercolatorIndex implements PercolatorIndex {

    public MultiDocumentPercolatorIndex() {
    }

    @Override
    public void prepare(PercolateContext context, ParsedDocument parsedDocument) {
        int docCounter = 0;
        IndexReader[] memoryIndices = new IndexReader[parsedDocument.docs().size()];
        for (ParseContext.Document d : parsedDocument.docs()) {
            memoryIndices[docCounter] = indexDoc(d, parsedDocument.analyzer()).createSearcher().getIndexReader();
            docCounter++;
        }
        MultiReader mReader = new MultiReader(memoryIndices, true);
        try {
            AtomicReader slowReader = SlowCompositeReaderWrapper.wrap(mReader);
            DocSearcher docSearcher = new DocSearcher(new IndexSearcher(slowReader));
            context.initialize(docSearcher, parsedDocument);
        } catch (IOException e) {
            throw new ElasticsearchException("Failed to create index for percolator with nested document ", e);
        }
    }

    MemoryIndex indexDoc(ParseContext.Document d, Analyzer analyzer) {
        MemoryIndex memoryIndex = new MemoryIndex(true);
        for (IndexableField field : d.getFields()) {
            if (!field.fieldType().indexed() && field.name().equals(UidFieldMapper.NAME)) {
                continue;
            }
            try {
                TokenStream tokenStream = field.tokenStream(analyzer);
                if (tokenStream != null) {
                    memoryIndex.addField(field.name(), tokenStream, field.boost());
                }
            } catch (IOException e) {
                throw new ElasticsearchException("Failed to create token stream", e);
            }
        }
        return memoryIndex;
    }

    @Override
    public void clean() {
        // noop
    }

    private class DocSearcher implements Engine.Searcher {

        private final IndexSearcher searcher;

        private DocSearcher(IndexSearcher searcher) {
            this.searcher = searcher;
        }

        @Override
        public String source() {
            return "percolate";
        }

        @Override
        public IndexReader reader() {
            return searcher.getIndexReader();
        }

        @Override
        public IndexSearcher searcher() {
            return searcher;
        }

        @Override
        public boolean release() throws ElasticsearchException {
            try {
                searcher.getIndexReader().close();
            } catch (IOException e) {
                throw new ElasticsearchException("failed to close IndexReader in percolator with nested doc", e);
            }
            return true;
        }

    }
}
