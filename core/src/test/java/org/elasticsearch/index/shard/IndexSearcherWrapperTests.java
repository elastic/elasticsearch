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
package org.elasticsearch.index.shard;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldFilterLeafReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class IndexSearcherWrapperTests extends ESTestCase {

    public void testReaderCloseListenerIsCalled() throws IOException {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig();
        IndexWriter writer = new IndexWriter(dir, iwc);
        Document doc = new Document();
        doc.add(new StringField("id", "1", random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
        doc.add(new TextField("field", "doc", random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
        writer.addDocument(doc);
        DirectoryReader open = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "_na_", 1));
        IndexSearcher searcher = new IndexSearcher(open);
        assertEquals(1, searcher.search(new TermQuery(new Term("field", "doc")), 1).totalHits);
        final AtomicInteger closeCalls = new AtomicInteger(0);
        IndexSearcherWrapper wrapper = new IndexSearcherWrapper() {
            @Override
            public DirectoryReader wrap(DirectoryReader reader) throws IOException {
                return new FieldMaskingReader("field", reader, closeCalls);
            }

            @Override
            public IndexSearcher wrap(IndexSearcher searcher) throws EngineException {
                return searcher;
            }

        };
        final int sourceRefCount = open.getRefCount();
        final AtomicInteger count = new AtomicInteger();
        final AtomicInteger outerCount = new AtomicInteger();
        try (Engine.Searcher engineSearcher = new Engine.Searcher("foo", searcher)) {
            final Engine.Searcher wrap =  wrapper.wrap(engineSearcher);
            assertEquals(1, wrap.reader().getRefCount());
            ElasticsearchDirectoryReader.addReaderCloseListener(wrap.getDirectoryReader(), reader -> {
                if (reader == open) {
                    count.incrementAndGet();
                }
                outerCount.incrementAndGet();
            });
            assertEquals(0, wrap.searcher().search(new TermQuery(new Term("field", "doc")), 1).totalHits);
            wrap.close();
            assertFalse("wrapped reader is closed", wrap.reader().tryIncRef());
            assertEquals(sourceRefCount, open.getRefCount());
        }
        assertEquals(1, closeCalls.get());

        IOUtils.close(open, writer, dir);
        assertEquals(1, outerCount.get());
        assertEquals(1, count.get());
        assertEquals(0, open.getRefCount());
        assertEquals(1, closeCalls.get());
    }

    public void testIsCacheable() throws IOException {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig();
        IndexWriter writer = new IndexWriter(dir, iwc);
        Document doc = new Document();
        doc.add(new StringField("id", "1", random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
        doc.add(new TextField("field", "doc", random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
        writer.addDocument(doc);
        DirectoryReader open = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "_na_", 1));
        IndexSearcher searcher = new IndexSearcher(open);
        assertEquals(1, searcher.search(new TermQuery(new Term("field", "doc")), 1).totalHits);
        searcher.setSimilarity(iwc.getSimilarity());
        final AtomicInteger closeCalls = new AtomicInteger(0);
        IndexSearcherWrapper wrapper = new IndexSearcherWrapper() {
            @Override
            public DirectoryReader wrap(DirectoryReader reader) throws IOException {
                return new FieldMaskingReader("field", reader, closeCalls);
            }

            @Override
            public IndexSearcher wrap(IndexSearcher searcher) throws EngineException {
                return searcher;
            }
        };
        final ConcurrentHashMap<Object, TopDocs> cache = new ConcurrentHashMap<>();
        try (Engine.Searcher engineSearcher = new Engine.Searcher("foo", searcher)) {
            try (final Engine.Searcher wrap = wrapper.wrap(engineSearcher)) {
                ElasticsearchDirectoryReader.addReaderCloseListener(wrap.getDirectoryReader(), reader -> {
                    cache.remove(reader.getCoreCacheKey());
                });
                TopDocs search = wrap.searcher().search(new TermQuery(new Term("field", "doc")), 1);
                cache.put(wrap.reader().getCoreCacheKey(), search);
            }
        }
        assertEquals(1, closeCalls.get());

        assertEquals(1, cache.size());
        IOUtils.close(open, writer, dir);
        assertEquals(0, cache.size());
        assertEquals(1, closeCalls.get());
    }

    public void testNoWrap() throws IOException {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig();
        IndexWriter writer = new IndexWriter(dir, iwc);
        Document doc = new Document();
        doc.add(new StringField("id", "1", random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
        doc.add(new TextField("field", "doc", random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
        writer.addDocument(doc);
        DirectoryReader open = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "_na_", 1));
        IndexSearcher searcher = new IndexSearcher(open);
        assertEquals(1, searcher.search(new TermQuery(new Term("field", "doc")), 1).totalHits);
        searcher.setSimilarity(iwc.getSimilarity());
        IndexSearcherWrapper wrapper = new IndexSearcherWrapper();
        try (Engine.Searcher engineSearcher = new Engine.Searcher("foo", searcher)) {
            final Engine.Searcher wrap = wrapper.wrap(engineSearcher);
            assertSame(wrap, engineSearcher);
        }
        IOUtils.close(open, writer, dir);
    }

    public void testWrappedReaderMustDelegateCoreCacheKey() throws IOException {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig();
        IndexWriter writer = new IndexWriter(dir, iwc);
        Document doc = new Document();
        doc.add(new StringField("id", "1", random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
        doc.add(new TextField("field", "doc", random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
        writer.addDocument(doc);
        DirectoryReader open = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "_na_", 1));
        IndexSearcher searcher = new IndexSearcher(open);
        assertEquals(1, searcher.search(new TermQuery(new Term("field", "doc")), 1).totalHits);
        searcher.setSimilarity(iwc.getSimilarity());
        IndexSearcherWrapper wrapper = new IndexSearcherWrapper() {
            @Override
            protected DirectoryReader wrap(DirectoryReader reader) throws IOException {
                return new BrokenWrapper(reader, false);
            }
        };
        try (Engine.Searcher engineSearcher = new Engine.Searcher("foo", searcher)) {
            try {
                wrapper.wrap(engineSearcher);
                fail("reader must delegate cache key");
            } catch (IllegalStateException ex) {
                // all is well
            }
        }
        wrapper = new IndexSearcherWrapper() {
            @Override
            protected DirectoryReader wrap(DirectoryReader reader) throws IOException {
                return new BrokenWrapper(reader, true);
            }
        };
        try (Engine.Searcher engineSearcher = new Engine.Searcher("foo", searcher)) {
            try {
                wrapper.wrap(engineSearcher);
                fail("reader must delegate cache key");
            } catch (IllegalStateException ex) {
                // all is well
            }
        }
        IOUtils.close(open, writer, dir);
    }

    private static class FieldMaskingReader extends FilterDirectoryReader {
        private final String field;
        private final AtomicInteger closeCalls;

        public FieldMaskingReader(String field, DirectoryReader in, AtomicInteger closeCalls) throws IOException {
            super(in, new SubReaderWrapper() {
                @Override
                public LeafReader wrap(LeafReader reader) {
                    return new FieldFilterLeafReader(reader, Collections.singleton(field), true);
                }
            });
            this.closeCalls = closeCalls;
            this.field = field;
        }

        @Override
        protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
            return new FieldMaskingReader(field, in, closeCalls);
        }

        @Override
        public Object getCoreCacheKey() {
            return in.getCoreCacheKey();
        }

        @Override
        protected void doClose() throws IOException {
            super.doClose();
            closeCalls.incrementAndGet();
        }
    }

    private static class BrokenWrapper extends FilterDirectoryReader {

        private final boolean hideDelegate;

        public BrokenWrapper(DirectoryReader in, boolean hideDelegate) throws IOException {
            super(in, new SubReaderWrapper() {
                @Override
                public LeafReader wrap(LeafReader reader) {
                    return reader;
                }
            });
            this.hideDelegate = hideDelegate;
        }

        @Override
        protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
            return new BrokenWrapper(in, hideDelegate);
        }

        @Override
        public DirectoryReader getDelegate() {
            if (hideDelegate) {
                try {
                    return ElasticsearchDirectoryReader.wrap(super.getDelegate(), new ShardId("foo", "_na_", 1));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            return super.getDelegate();
        }

        @Override
        public Object getCoreCacheKey() {
            if (hideDelegate == false) {
                return super.getCoreCacheKey();
            } else {
                return in.getCoreCacheKey();
            }
        }
    }
}
