/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.search.stats.ShardFieldUsageTracker;
import org.elasticsearch.search.internal.FieldUsageTrackingDirectoryReader;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

public class IndexReaderWrapperTests extends ESTestCase {

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
        assertEquals(1, searcher.search(new TermQuery(new Term("field", "doc")), 1).totalHits.value);
        final AtomicInteger closeCalls = new AtomicInteger(0);
        CheckedFunction<DirectoryReader, DirectoryReader, IOException> wrapper =
            reader -> new FieldMaskingReader("field", reader, closeCalls);
        final int sourceRefCount = open.getRefCount();
        final AtomicInteger count = new AtomicInteger();
        final AtomicInteger outerCount = new AtomicInteger();
        final AtomicBoolean closeCalled = new AtomicBoolean(false);
        final Engine.Searcher wrap =  IndexShard.wrapSearcher(new Engine.Searcher("foo", open,
            IndexSearcher.getDefaultSimilarity(), IndexSearcher.getDefaultQueryCache(), IndexSearcher.getDefaultQueryCachingPolicy(),
            () -> closeCalled.set(true)), mock(ShardFieldUsageTracker.FieldUsageStatsTrackingSession.class), wrapper);
        assertEquals(1, wrap.getIndexReader().getRefCount());
        ElasticsearchDirectoryReader.addReaderCloseListener(wrap.getDirectoryReader(), key -> {
            if (key == open.getReaderCacheHelper().getKey()) {
                count.incrementAndGet();
            }
            outerCount.incrementAndGet();
        });
        assertEquals(0, wrap.search(new TermQuery(new Term("field", "doc")), 1).totalHits.value);
        wrap.close();
        assertFalse("wrapped reader is closed", wrap.getIndexReader().tryIncRef());
        assertEquals(sourceRefCount, open.getRefCount());
        assertTrue(closeCalled.get());
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
        assertEquals(1, searcher.search(new TermQuery(new Term("field", "doc")), 1).totalHits.value);
        searcher.setSimilarity(iwc.getSimilarity());
        final AtomicInteger closeCalls = new AtomicInteger(0);
        CheckedFunction<DirectoryReader, DirectoryReader, IOException> wrapper =
            reader -> new FieldMaskingReader("field", reader, closeCalls);
        final ConcurrentHashMap<Object, TopDocs> cache = new ConcurrentHashMap<>();
        AtomicBoolean closeCalled = new AtomicBoolean(false);
        try (Engine.Searcher wrap =  IndexShard.wrapSearcher(new Engine.Searcher("foo", open,
                IndexSearcher.getDefaultSimilarity(), IndexSearcher.getDefaultQueryCache(), IndexSearcher.getDefaultQueryCachingPolicy(),
                () -> closeCalled.set(true)), mock(ShardFieldUsageTracker.FieldUsageStatsTrackingSession.class), wrapper)) {
            ElasticsearchDirectoryReader.addReaderCloseListener(wrap.getDirectoryReader(), key -> {
                cache.remove(key);
            });
            TopDocs search = wrap.search(new TermQuery(new Term("field", "doc")), 1);
            cache.put(wrap.getIndexReader().getReaderCacheHelper().getKey(), search);
        }
        assertTrue(closeCalled.get());
        assertEquals(1, closeCalls.get());

        assertEquals(1, cache.size());
        IOUtils.close(open, writer, dir);
        assertEquals(0, cache.size());
        assertEquals(1, closeCalls.get());
    }

    public void testAlwaysWrapWithFieldUsageTrackingDirectoryReader() throws IOException {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig();
        IndexWriter writer = new IndexWriter(dir, iwc);
        Document doc = new Document();
        doc.add(new StringField("id", "1", random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
        doc.add(new TextField("field", "doc", random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
        writer.addDocument(doc);
        DirectoryReader open = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "_na_", 1));
        IndexSearcher searcher = new IndexSearcher(open);
        assertEquals(1, searcher.search(new TermQuery(new Term("field", "doc")), 1).totalHits.value);
        searcher.setSimilarity(iwc.getSimilarity());
        CheckedFunction<DirectoryReader, DirectoryReader, IOException> wrapper = directoryReader -> directoryReader;
        try (Engine.Searcher engineSearcher =  IndexShard.wrapSearcher(new Engine.Searcher("foo", open,
                IndexSearcher.getDefaultSimilarity(), IndexSearcher.getDefaultQueryCache(), IndexSearcher.getDefaultQueryCachingPolicy(),
                open::close), mock(ShardFieldUsageTracker.FieldUsageStatsTrackingSession.class), wrapper)) {
            final Engine.Searcher wrap = IndexShard.wrapSearcher(engineSearcher,
                mock(ShardFieldUsageTracker.FieldUsageStatsTrackingSession.class), wrapper);
            assertNotSame(wrap, engineSearcher);
            assertThat(wrap.getDirectoryReader(), instanceOf(FieldUsageTrackingDirectoryReader.class));
        }
        IOUtils.close(writer, dir);
    }

    private static class FieldMaskingReader extends FilterDirectoryReader {
        private final String field;
        private final AtomicInteger closeCalls;

        FieldMaskingReader(String field, DirectoryReader in, AtomicInteger closeCalls) throws IOException {
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
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }

        @Override
        protected void doClose() throws IOException {
            super.doClose();
            closeCalls.incrementAndGet();
        }
    }

}
