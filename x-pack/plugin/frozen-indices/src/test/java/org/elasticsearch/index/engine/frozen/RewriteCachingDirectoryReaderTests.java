/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.index.engine.frozen;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.IndexType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.time.ZoneOffset;
import java.util.Map;

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
                        doc.add(NumericDocValuesField.indexedField("rarely_skipper", 1));
                    } else {
                        long value = randomLongBetween(0, 10000);
                        doc.add(new LongPoint("test", value));
                        doc.add(new LongPoint("test_const", 1));
                        doc.add(NumericDocValuesField.indexedField("skipper", value));
                    }
                    writer.addDocument(doc);
                }
                try (DirectoryReader reader = DirectoryReader.open(writer)) {
                    RewriteCachingDirectoryReader cachingDirectoryReader = new RewriteCachingDirectoryReader(dir, reader.leaves(), null);
                    IndexSearcher searcher = new IndexSearcher(reader);
                    IndexSearcher cachingSearcher = new IndexSearcher(cachingDirectoryReader);

                    if (rarely) {
                        assertArrayEquals(
                            PointValues.getMaxPackedValue(reader, "rarely"),
                            PointValues.getMaxPackedValue(cachingDirectoryReader, "rarely")
                        );
                        assertArrayEquals(
                            PointValues.getMinPackedValue(reader, "rarely"),
                            PointValues.getMinPackedValue(cachingDirectoryReader, "rarely")
                        );
                        assertEquals(PointValues.size(reader, "rarely"), PointValues.size(cachingDirectoryReader, "rarely"));
                        assertEquals(
                            DocValuesSkipper.globalDocCount(searcher, "rarely_skipper"),
                            DocValuesSkipper.globalDocCount(cachingSearcher, "rarely_skipper")
                        );
                        assertEquals(
                            DocValuesSkipper.globalMaxValue(searcher, "rarely_skipper"),
                            DocValuesSkipper.globalMaxValue(cachingSearcher, "rarely_skipper")
                        );
                        assertEquals(
                            DocValuesSkipper.globalMinValue(searcher, "rarely_skipper"),
                            DocValuesSkipper.globalMinValue(cachingSearcher, "rarely_skipper")
                        );
                    }
                    assertArrayEquals(
                        PointValues.getMaxPackedValue(reader, "test"),
                        PointValues.getMaxPackedValue(cachingDirectoryReader, "test")
                    );
                    assertArrayEquals(
                        PointValues.getMaxPackedValue(reader, "test_const"),
                        PointValues.getMaxPackedValue(cachingDirectoryReader, "test_const")
                    );

                    assertArrayEquals(
                        PointValues.getMinPackedValue(reader, "test"),
                        PointValues.getMinPackedValue(cachingDirectoryReader, "test")
                    );
                    assertArrayEquals(
                        PointValues.getMinPackedValue(reader, "test_const"),
                        PointValues.getMinPackedValue(cachingDirectoryReader, "test_const")
                    );

                    assertEquals(PointValues.size(reader, "test"), PointValues.size(cachingDirectoryReader, "test"));
                    assertEquals(PointValues.size(reader, "test_const"), PointValues.size(cachingDirectoryReader, "test_const"));

                    assertEquals(
                        DocValuesSkipper.globalDocCount(searcher, "skipper"),
                        DocValuesSkipper.globalDocCount(cachingSearcher, "skipper")
                    );
                    assertEquals(
                        DocValuesSkipper.globalMinValue(searcher, "skipper"),
                        DocValuesSkipper.globalMinValue(cachingSearcher, "skipper")
                    );
                    assertEquals(
                        DocValuesSkipper.globalMaxValue(searcher, "skipper"),
                        DocValuesSkipper.globalMaxValue(cachingSearcher, "skipper")
                    );
                }
            }
        }
    }

    public void testIsWithinQuery() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig())) {
                Document doc = new Document();
                doc.add(new LongPoint("test", 5));
                doc.add(NumericDocValuesField.indexedField("skipper", 5));
                writer.addDocument(doc);
                if (randomBoolean()) {
                    writer.flush();
                }
                doc = new Document();
                doc.add(new LongPoint("test", 0));
                doc.add(NumericDocValuesField.indexedField("skipper", 0));
                writer.addDocument(doc);
                if (randomBoolean()) {
                    writer.flush();
                }
                doc = new Document();
                doc.add(new LongPoint("test", 10));
                doc.add(NumericDocValuesField.indexedField("skipper", 10));
                writer.addDocument(doc);
                try (DirectoryReader reader = DirectoryReader.open(writer)) {
                    RewriteCachingDirectoryReader cachingDirectoryReader = new RewriteCachingDirectoryReader(dir, reader.leaves(), null);
                    assertRelations(new DateFieldMapper.DateFieldType("test"), cachingDirectoryReader);
                    assertRelations(
                        new DateFieldMapper.DateFieldType(
                            "skipper",
                            IndexType.skippers(),
                            false,
                            false,
                            DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER,
                            DateFieldMapper.Resolution.MILLISECONDS,
                            null,
                            null,
                            Map.of()
                        ),
                        cachingDirectoryReader
                    );
                }
            }
        }
    }

    private void assertRelations(DateFieldMapper.DateFieldType dateFieldType, IndexReader reader) throws IOException {
        QueryRewriteContext context = new QueryRewriteContext(parserConfig(), null, () -> 0);
        MappedFieldType.Relation relation = dateFieldType.isFieldWithinQuery(reader, 0, 10, true, true, ZoneOffset.UTC, null, context);
        assertEquals(MappedFieldType.Relation.WITHIN, relation);

        relation = dateFieldType.isFieldWithinQuery(reader, 3, 11, true, true, ZoneOffset.UTC, null, context);
        assertEquals(MappedFieldType.Relation.INTERSECTS, relation);

        relation = dateFieldType.isFieldWithinQuery(reader, 10, 11, false, true, ZoneOffset.UTC, null, context);
        assertEquals(MappedFieldType.Relation.DISJOINT, relation);
    }
}
