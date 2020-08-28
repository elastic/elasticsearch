/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.index.engine;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.store.Directory;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.time.ZoneOffset;

public class RewriteCachingDirectoryReaderTests extends ESTestCase {

    public void testGetMinMaxPackedValue() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig())) {
                int numDocs = randomIntBetween(10, 100);
                boolean rarely = false;
                for (int i = 0; i < numDocs; i++) {
                    Document doc = new Document();
                    if (i > 0 && rarely()) {
                        rarely = true;
                        doc.add(new LongPoint("rarely", 1));
                    } else {
                        long value = randomLongBetween(0, 10000);
                        doc.add(new LongPoint("test", value));
                        doc.add(new LongPoint("test_const", 1));
                    }
                    writer.addDocument(doc);
                }
                try (DirectoryReader reader = DirectoryReader.open(writer)) {
                    RewriteCachingDirectoryReader cachingDirectoryReader = new RewriteCachingDirectoryReader(dir, reader.leaves());
                    if (rarely) {
                        assertArrayEquals(PointValues.getMaxPackedValue(reader, "rarely"),
                            PointValues.getMaxPackedValue(cachingDirectoryReader, "rarely"));
                        assertArrayEquals(PointValues.getMinPackedValue(reader, "rarely"),
                            PointValues.getMinPackedValue(cachingDirectoryReader, "rarely"));
                        assertEquals(PointValues.size(reader, "rarely"),
                            PointValues.size(cachingDirectoryReader, "rarely"));
                    }
                    assertArrayEquals(PointValues.getMaxPackedValue(reader, "test"),
                        PointValues.getMaxPackedValue(cachingDirectoryReader, "test"));
                    assertArrayEquals(PointValues.getMaxPackedValue(reader, "test_const"),
                        PointValues.getMaxPackedValue(cachingDirectoryReader, "test_const"));

                    assertArrayEquals(PointValues.getMinPackedValue(reader, "test"),
                        PointValues.getMinPackedValue(cachingDirectoryReader, "test"));
                    assertArrayEquals(PointValues.getMinPackedValue(reader, "test_const"),
                        PointValues.getMinPackedValue(cachingDirectoryReader, "test_const"));

                    assertEquals(PointValues.size(reader, "test"),
                        PointValues.size(cachingDirectoryReader, "test"));
                    assertEquals(PointValues.size(reader, "test_const"),
                        PointValues.size(cachingDirectoryReader, "test_const"));
                }
            }
        }
    }

    public void testIsWithinQuery() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig())) {
                Document doc = new Document();
                doc.add(new LongPoint("test", 5));
                writer.addDocument(doc);
                if (randomBoolean()) {
                    writer.flush();
                }
                doc = new Document();
                doc.add(new LongPoint("test", 0));
                writer.addDocument(doc);
                if (randomBoolean()) {
                    writer.flush();
                }
                doc = new Document();
                doc.add(new LongPoint("test", 10));
                writer.addDocument(doc);
                try (DirectoryReader reader = DirectoryReader.open(writer)) {
                    RewriteCachingDirectoryReader cachingDirectoryReader = new RewriteCachingDirectoryReader(dir, reader.leaves());
                    DateFieldMapper.DateFieldType dateFieldType = new DateFieldMapper.DateFieldType("test");
                    QueryRewriteContext context = new QueryRewriteContext(xContentRegistry(), writableRegistry(), null, () -> 0);
                    MappedFieldType.Relation relation = dateFieldType.isFieldWithinQuery(cachingDirectoryReader, 0, 10,
                        true, true, ZoneOffset.UTC, null, context);
                    assertEquals(relation, MappedFieldType.Relation.WITHIN);

                    relation = dateFieldType.isFieldWithinQuery(cachingDirectoryReader, 3, 11,
                        true, true, ZoneOffset.UTC, null, context);
                    assertEquals(relation, MappedFieldType.Relation.INTERSECTS);

                    relation = dateFieldType.isFieldWithinQuery(cachingDirectoryReader, 10, 11,
                        false, true, ZoneOffset.UTC, null, context);
                    assertEquals(relation, MappedFieldType.Relation.DISJOINT);
                }
            }
        }
    }
}
