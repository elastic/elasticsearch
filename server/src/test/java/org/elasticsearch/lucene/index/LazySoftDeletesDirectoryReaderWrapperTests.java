/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.index;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SoftDeletesRetentionMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.engine.LazySoftDeletesDirectoryReaderWrapper;
import org.elasticsearch.test.GraalVMThreadsFilter;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

@ThreadLeakFilters(filters = { GraalVMThreadsFilter.class })
public class LazySoftDeletesDirectoryReaderWrapperTests extends LuceneTestCase {

    public void testDropFullyDeletedSegments() throws IOException {
        IndexWriterConfig indexWriterConfig = newIndexWriterConfig();
        String softDeletesField = "soft_delete";
        indexWriterConfig.setSoftDeletesField(softDeletesField);
        indexWriterConfig.setMergePolicy(
            new SoftDeletesRetentionMergePolicy(softDeletesField, () -> Queries.ALL_DOCS_INSTANCE, NoMergePolicy.INSTANCE)
        );
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, indexWriterConfig)) {

            Document doc = new Document();
            doc.add(new StringField("id", "1", Field.Store.YES));
            doc.add(new StringField("version", "1", Field.Store.YES));
            writer.addDocument(doc);
            writer.commit();
            doc = new Document();
            doc.add(new StringField("id", "2", Field.Store.YES));
            doc.add(new StringField("version", "1", Field.Store.YES));
            writer.addDocument(doc);
            writer.commit();

            try (DirectoryReader reader = new LazySoftDeletesDirectoryReaderWrapper(DirectoryReader.open(dir), softDeletesField)) {
                assertEquals(2, reader.leaves().size());
                assertEquals(2, reader.numDocs());
                assertEquals(2, reader.maxDoc());
                assertEquals(0, reader.numDeletedDocs());
            }
            writer.updateDocValues(new Term("id", "1"), new NumericDocValuesField(softDeletesField, 1));
            writer.commit();
            try (DirectoryReader reader = new LazySoftDeletesDirectoryReaderWrapper(DirectoryReader.open(dir), softDeletesField)) {
                assertEquals(1, reader.numDocs());
                assertEquals(1, reader.maxDoc());
                assertEquals(0, reader.numDeletedDocs());
                assertEquals(1, reader.leaves().size());
            }

            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertEquals(2, reader.numDocs());
                assertEquals(2, reader.maxDoc());
                assertEquals(0, reader.numDeletedDocs());
                assertEquals(2, reader.leaves().size());
            }
        }
    }

    public void testMixSoftAndHardDeletes() throws IOException {
        Directory dir = newDirectory();
        IndexWriterConfig indexWriterConfig = newIndexWriterConfig();
        String softDeletesField = "soft_delete";
        indexWriterConfig.setSoftDeletesField(softDeletesField);
        IndexWriter writer = new IndexWriter(dir, indexWriterConfig);
        Set<Integer> uniqueDocs = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            int docId = random().nextInt(5);
            uniqueDocs.add(docId);
            Document doc = new Document();
            doc.add(new StringField("id", String.valueOf(docId), Field.Store.YES));
            if (docId % 2 == 0) {
                writer.updateDocument(new Term("id", String.valueOf(docId)), doc);
            } else {
                writer.softUpdateDocument(new Term("id", String.valueOf(docId)), doc, new NumericDocValuesField(softDeletesField, 0));
            }
        }

        writer.commit();
        writer.close();
        DirectoryReader reader = new LazySoftDeletesDirectoryReaderWrapper(DirectoryReader.open(dir), softDeletesField);
        assertEquals(uniqueDocs.size(), reader.numDocs());
        IndexSearcher searcher = newSearcher(reader);
        for (Integer docId : uniqueDocs) {
            assertEquals(1, searcher.count(new TermQuery(new Term("id", docId.toString()))));
        }

        IOUtils.close(reader, dir);
    }

    public void testReopenAfterNewCommit() throws IOException {
        IndexWriterConfig indexWriterConfig = newIndexWriterConfig();
        String softDeletesField = "soft_delete";
        indexWriterConfig.setSoftDeletesField(softDeletesField);
        indexWriterConfig.setMergePolicy(NoMergePolicy.INSTANCE);
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, indexWriterConfig)) {
            // Three segments of two docs each so a single soft delete leaves the segment with one live doc.
            for (int seg = 0; seg < 3; seg++) {
                for (int d = 0; d < 2; d++) {
                    Document doc = new Document();
                    doc.add(new StringField("id", seg + "-" + d, Field.Store.YES));
                    writer.addDocument(doc);
                }
                writer.commit();
            }

            DirectoryReader reader = new LazySoftDeletesDirectoryReaderWrapper(DirectoryReader.open(dir), softDeletesField);
            try {
                assertEquals(3, reader.leaves().size());
                assertEquals(6, reader.numDocs());

                writer.updateDocValues(new Term("id", "1-0"), new NumericDocValuesField(softDeletesField, 1));
                writer.commit();

                DirectoryReader reopened = DirectoryReader.openIfChanged(reader);
                assertNotNull("openIfChanged should re-wrap with LazySoftDeletesDirectoryReaderWrapper", reopened);
                try {
                    assertTrue(reopened instanceof LazySoftDeletesDirectoryReaderWrapper);
                    assertEquals(3, reopened.leaves().size());
                    assertEquals(5, reopened.numDocs());
                    assertEquals(1, reopened.numDeletedDocs());
                } finally {
                    reopened.close();
                }
            } finally {
                reader.close();
            }
        }
    }

    public void testReopenSharesUnchangedLeaves() throws IOException {
        IndexWriterConfig indexWriterConfig = newIndexWriterConfig();
        String softDeletesField = "soft_delete";
        indexWriterConfig.setSoftDeletesField(softDeletesField);
        indexWriterConfig.setMergePolicy(NoMergePolicy.INSTANCE);
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, indexWriterConfig)) {
            for (int seg = 0; seg < 3; seg++) {
                for (int d = 0; d < 2; d++) {
                    Document doc = new Document();
                    doc.add(new StringField("id", seg + "-" + d, Field.Store.YES));
                    writer.addDocument(doc);
                }
                writer.commit();
            }

            DirectoryReader reader = new LazySoftDeletesDirectoryReaderWrapper(DirectoryReader.open(dir), softDeletesField);
            try {
                // Touch only segment 1 with a soft delete. Segments 0 and 2 are unchanged and must share their reader cache key
                // (i.e. the same wrapped LeafReader instance, which is what the future budget tracker relies on to identify
                // shared bitsets across reader generations). Segment 1's reader cache key must differ because a new soft-delete
                // generation produces a new SegmentReader with its own LazyBits / FixedBitSet.
                writer.updateDocValues(new Term("id", "1-0"), new NumericDocValuesField(softDeletesField, 1));
                writer.commit();

                DirectoryReader reopened = DirectoryReader.openIfChanged(reader);
                assertNotNull(reopened);
                try {
                    Set<IndexReader.CacheKey> originalReaderKeys = new HashSet<>();
                    for (var ctx : reader.leaves()) {
                        originalReaderKeys.add(ctx.reader().getReaderCacheHelper().getKey());
                    }
                    int shared = 0;
                    for (var ctx : reopened.leaves()) {
                        if (originalReaderKeys.contains(ctx.reader().getReaderCacheHelper().getKey())) {
                            shared++;
                        }
                    }
                    assertEquals("two unchanged segments must share their reader cache key across reopen", 2, shared);
                } finally {
                    reopened.close();
                }
            } finally {
                reader.close();
            }
        }
    }

    public void testReaderCacheKey() throws IOException {
        Directory dir = newDirectory();
        IndexWriterConfig indexWriterConfig = newIndexWriterConfig();
        String softDeletesField = "soft_delete";
        indexWriterConfig.setSoftDeletesField(softDeletesField);
        indexWriterConfig.setMergePolicy(NoMergePolicy.INSTANCE);
        IndexWriter writer = new IndexWriter(dir, indexWriterConfig);

        Document doc = new Document();
        doc.add(new StringField("id", "1", Field.Store.YES));
        doc.add(new StringField("version", "1", Field.Store.YES));
        writer.addDocument(doc);
        doc = new Document();
        doc.add(new StringField("id", "2", Field.Store.YES));
        doc.add(new StringField("version", "1", Field.Store.YES));
        writer.addDocument(doc);
        writer.commit();
        DirectoryReader reader = new LazySoftDeletesDirectoryReaderWrapper(DirectoryReader.open(dir), softDeletesField);
        IndexReader.CacheHelper readerCacheHelper = reader.leaves().get(0).reader().getReaderCacheHelper();
        AtomicInteger leafCalled = new AtomicInteger(0);
        AtomicInteger dirCalled = new AtomicInteger(0);
        readerCacheHelper.addClosedListener(key -> {
            leafCalled.incrementAndGet();
            assertSame(key, readerCacheHelper.getKey());
        });
        IndexReader.CacheHelper dirReaderCacheHelper = reader.getReaderCacheHelper();
        dirReaderCacheHelper.addClosedListener(key -> {
            dirCalled.incrementAndGet();
            assertSame(key, dirReaderCacheHelper.getKey());
        });
        assertEquals(2, reader.numDocs());
        assertEquals(2, reader.maxDoc());
        assertEquals(0, reader.numDeletedDocs());

        doc = new Document();
        doc.add(new StringField("id", "1", Field.Store.YES));
        doc.add(new StringField("version", "2", Field.Store.YES));
        writer.softUpdateDocument(new Term("id", "1"), doc, new NumericDocValuesField("soft_delete", 1));

        doc = new Document();
        doc.add(new StringField("id", "3", Field.Store.YES));
        doc.add(new StringField("version", "1", Field.Store.YES));
        writer.addDocument(doc);
        writer.commit();
        assertEquals(0, leafCalled.get());
        assertEquals(0, dirCalled.get());
        DirectoryReader newReader = new LazySoftDeletesDirectoryReaderWrapper(DirectoryReader.open(dir), softDeletesField);
        assertEquals(0, leafCalled.get());
        assertEquals(0, dirCalled.get());
        assertNotSame(newReader.getReaderCacheHelper().getKey(), reader.getReaderCacheHelper().getKey());
        assertNotSame(newReader, reader);
        reader.close();
        reader = newReader;
        assertEquals(1, dirCalled.get());
        assertEquals(1, leafCalled.get());
        IOUtils.close(reader, writer, dir);
    }
}
