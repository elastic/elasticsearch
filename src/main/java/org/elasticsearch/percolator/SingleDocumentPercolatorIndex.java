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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.memory.ExtendedMemoryIndex;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.CloseableThreadLocal;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;

import java.io.IOException;

class SingleDocumentPercolatorIndex implements PercolatorIndex {

    private final CloseableThreadLocal<MemoryIndex> cache;

    public SingleDocumentPercolatorIndex(Settings settings) {
        final long maxReuseBytes = settings.getAsBytesSize("indices.memory.memory_index.size_per_thread", new ByteSizeValue(1, ByteSizeUnit.MB)).bytes();
        cache = new CloseableThreadLocal<MemoryIndex>() {
            @Override
            protected MemoryIndex initialValue() {
                return new ExtendedMemoryIndex(true, maxReuseBytes);
            }
        };
    }

    @Override
    public void prepare(PercolateContext context, ParsedDocument parsedDocument) {
        MemoryIndex memoryIndex = cache.get();
        for (IndexableField field : parsedDocument.rootDoc().getFields()) {
            if (!field.fieldType().indexed() && field.name().equals(UidFieldMapper.NAME)) {
                continue;
            }
            try {
                TokenStream tokenStream = field.tokenStream(parsedDocument.analyzer());
                if (tokenStream != null) {
                    memoryIndex.addField(field.name(), tokenStream, field.boost());
                }
            } catch (IOException e) {
                throw new ElasticsearchException("Failed to create token stream", e);
            }
        }
        context.initialize(new DocEngineSearcher(memoryIndex), parsedDocument);
    }

    @Override
    public void clean() {
        cache.close();
    }

    private class DocEngineSearcher implements Engine.Searcher {

        private final IndexSearcher searcher;
        private final MemoryIndex memoryIndex;

        public DocEngineSearcher(MemoryIndex memoryIndex) {
            this.searcher = memoryIndex.createSearcher();
            this.memoryIndex = memoryIndex;
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
                memoryIndex.reset();
            } catch (IOException e) {
                throw new ElasticsearchException("failed to close percolator in-memory index", e);
            }
            return true;
        }
    }
}
