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

import org.apache.lucene.index.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.bytebuffer.ByteBufferDirectory;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.ParsedDocument;

import java.io.IOException;


class MultiDocumentPercolatorIndex implements PercolatorIndex {
    private final IndexSearcher docSearcher;
    private final Engine.Searcher docEngineSearcher;
    private final IndexReader topLevelReader;
    private final AtomicReaderContext readerContext;

    public MultiDocumentPercolatorIndex(ParsedDocument parsedDocument) {
        ByteBufferDirectory directory = new ByteBufferDirectory();
        IndexWriterConfig writerConfig = new IndexWriterConfig(Version.CURRENT.luceneVersion, parsedDocument.analyzer());
        try {
            IndexWriter indexWriter = new IndexWriter(directory, writerConfig);
            indexWriter.addDocuments(parsedDocument.docs());
            docSearcher = new IndexSearcher(DirectoryReader.open(indexWriter, false));
            indexWriter.close();
        } catch (IOException e) {
            throw new ElasticsearchException("Failed to create index for percolator with nested document ", e);
        }

        topLevelReader = docSearcher.getIndexReader();
        readerContext = topLevelReader.leaves().get(0);
        docEngineSearcher = new Engine.Searcher() {
            @Override
            public String source() {
                return "percolate";
            }

            @Override
            public IndexReader reader() {
                return topLevelReader;
            }

            @Override
            public IndexSearcher searcher() {
                return docSearcher;
            }

            @Override
            public boolean release() throws ElasticsearchException {
                try {
                    docSearcher.getIndexReader().close();
                } catch (IOException e) {
                    throw new ElasticsearchException("failed to close IndexReader in percolator with nested doc", e);
                }
                return true;
            }
        };

    }

    @Override
    public Engine.Searcher getSearcher() {
        return docEngineSearcher;
    }

    @Override
    public IndexReader getIndexReader() {
        return topLevelReader;
    }

    @Override
    public AtomicReaderContext getAtomicReaderContext() {
        return readerContext;
    }
}
