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
import org.apache.lucene.util.CloseableThreadLocal;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;

import java.io.IOException;
import java.util.List;


/**
 * Implementation of {@link PercolatorIndex} that can hold multiple Lucene documents by
 * opening multiple {@link MemoryIndex} based IndexReaders and wrapping them via a single top level reader.
 */
class MultiDocumentPercolatorIndex implements PercolatorIndex {

    private final CloseableThreadLocal<MemoryIndex> cache;

    MultiDocumentPercolatorIndex(CloseableThreadLocal<MemoryIndex> cache) {
        this.cache = cache;
    }

    @Override
    public void prepare(PercolateContext context, ParsedDocument parsedDocument) {
        IndexReader[] memoryIndices = new IndexReader[parsedDocument.docs().size()];
        List<ParseContext.Document> docs = parsedDocument.docs();
        int rootDocIndex = docs.size() - 1;
        assert rootDocIndex > 0;
        MemoryIndex rootDocMemoryIndex = null;
        for (int i = 0; i < docs.size(); i++) {
            ParseContext.Document d = docs.get(i);
            MemoryIndex memoryIndex;
            if (rootDocIndex == i) {
                // the last doc is always the rootDoc, since that is usually the biggest document it make sense
                // to reuse the MemoryIndex it uses
                memoryIndex = rootDocMemoryIndex = cache.get();
            } else {
                memoryIndex = new MemoryIndex(true);
            }
            Analyzer analyzer = context.mapperService().documentMapper(parsedDocument.type()).mappers().indexAnalyzer();
            memoryIndices[i] = indexDoc(d, analyzer, memoryIndex).createSearcher().getIndexReader();
        }
        try {
            MultiReader mReader = new MultiReader(memoryIndices, true);
            LeafReader slowReader = SlowCompositeReaderWrapper.wrap(mReader);
            final IndexSearcher slowSearcher = new IndexSearcher(slowReader);
            slowSearcher.setQueryCache(null);
            DocSearcher docSearcher = new DocSearcher(slowSearcher, rootDocMemoryIndex);
            context.initialize(docSearcher, parsedDocument);
        } catch (IOException e) {
            throw new ElasticsearchException("Failed to create index for percolator with nested document ", e);
        }
    }

    MemoryIndex indexDoc(ParseContext.Document d, Analyzer analyzer, MemoryIndex memoryIndex) {
        for (IndexableField field : d.getFields()) {
            if (field.fieldType().indexOptions() == IndexOptions.NONE && field.name().equals(UidFieldMapper.NAME)) {
                continue;
            }
            try {
                // TODO: instead of passing null here, we can have a CTL<Map<String,TokenStream>> and pass previous,
                // like the indexer does
                TokenStream tokenStream = field.tokenStream(analyzer, null);
                if (tokenStream != null) {
                    memoryIndex.addField(field.name(), tokenStream, field.boost());
                }
            } catch (IOException e) {
                throw new ElasticsearchException("Failed to create token stream", e);
            }
        }
        return memoryIndex;
    }

    private class DocSearcher extends Engine.Searcher {

        private final MemoryIndex rootDocMemoryIndex;

        private DocSearcher(IndexSearcher searcher, MemoryIndex rootDocMemoryIndex) {
            super("percolate", searcher);
            this.rootDocMemoryIndex = rootDocMemoryIndex;
        }

        @Override
        public void close() {
            try {
                this.reader().close();
                rootDocMemoryIndex.reset();
            } catch (IOException e) {
                throw new ElasticsearchException("failed to close IndexReader in percolator with nested doc", e);
            }
        }

    }
}
