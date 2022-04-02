/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.lucene.index;

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
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.engine.LazySoftDeletesDirectoryReaderWrapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class LazySoftDeletesDirectoryReaderWrapperTests extends LuceneTestCase {

    public void testDropFullyDeletedSegments() throws IOException {
        IndexWriterConfig indexWriterConfig = newIndexWriterConfig();
        String softDeletesField = "soft_delete";
        indexWriterConfig.setSoftDeletesField(softDeletesField);
        indexWriterConfig.setMergePolicy(
            new SoftDeletesRetentionMergePolicy(softDeletesField, MatchAllDocsQuery::new, NoMergePolicy.INSTANCE)
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
        IndexSearcher searcher = new IndexSearcher(reader);
        for (Integer docId : uniqueDocs) {
            assertEquals(1, searcher.count(new TermQuery(new Term("id", docId.toString()))));
        }

        IOUtils.close(reader, dir);
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
