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

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.bytebuffer.ByteBufferDirectory;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.ParsedDocument;

import java.io.IOException;


class MultiDocumentPercolatorIndex implements PercolatorIndex {

    public MultiDocumentPercolatorIndex() {
    }

    @Override
    public void prepare(PercolateContext context, ParsedDocument parsedDocument) {
        ByteBufferDirectory directory = new ByteBufferDirectory();
        IndexWriterConfig writerConfig = new IndexWriterConfig(Version.CURRENT.luceneVersion, parsedDocument.analyzer());
        try {
            IndexWriter indexWriter = new IndexWriter(directory, writerConfig);
            indexWriter.addDocuments(parsedDocument.docs());
            DocSearcher docSearcher = new DocSearcher(new IndexSearcher(DirectoryReader.open(indexWriter, false)), directory);
            context.initialize(docSearcher, parsedDocument);
            indexWriter.close();
        } catch (IOException e) {
            throw new ElasticsearchException("Failed to create index for percolator with nested document ", e);
        }
    }

    @Override
    public void clean() {
        // noop
    }

    private class DocSearcher implements Engine.Searcher {

        private final IndexSearcher searcher;
        private final Directory directory;

        private DocSearcher(IndexSearcher searcher, Directory directory) {
            this.searcher = searcher;
            this.directory = directory;
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
                directory.close();
            } catch (IOException e) {
                throw new ElasticsearchException("failed to close IndexReader in percolator with nested doc", e);
            }
            return true;
        }

    }
}
