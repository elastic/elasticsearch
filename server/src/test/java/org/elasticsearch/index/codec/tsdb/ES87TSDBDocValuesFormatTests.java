/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.BaseDocValuesFormatTestCase;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

public class ES87TSDBDocValuesFormatTests extends BaseDocValuesFormatTestCase {

    private static final int NUM_DOCS = 10;

    private final Codec codec = TestUtil.alwaysDocValuesFormat(new ES87TSDBDocValuesFormat());

    @Override
    protected Codec getCodec() {
        return codec;
    }

    public void testSortedDocValuesSingleUniqueValue() throws IOException {
        try (Directory directory = newDirectory()) {
            Analyzer analyzer = new MockAnalyzer(random());
            IndexWriterConfig conf = newIndexWriterConfig(analyzer);
            conf.setMergePolicy(newLogMergePolicy());
            try (RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, conf)) {
                for (int i = 0; i < NUM_DOCS; i++) {
                    Document doc = new Document();
                    doc.add(new SortedDocValuesField("field", newBytesRef("value")));
                    doc.add(new SortedDocValuesField("field" + i, newBytesRef("value" + i)));
                    iwriter.addDocument(doc);
                }
                iwriter.forceMerge(1);
            }
            try (IndexReader ireader = maybeWrapWithMergingReader(DirectoryReader.open(directory))) {
                assert ireader.leaves().size() == 1;
                SortedDocValues field = ireader.leaves().get(0).reader().getSortedDocValues("field");
                for (int i = 0; i < NUM_DOCS; i++) {
                    assertEquals(i, field.nextDoc());
                    assertEquals(0, field.ordValue());
                    BytesRef scratch = field.lookupOrd(0);
                    assertEquals("value", scratch.utf8ToString());
                }
                assertEquals(DocIdSetIterator.NO_MORE_DOCS, field.nextDoc());
                for (int i = 0; i < NUM_DOCS; i++) {
                    SortedDocValues fieldN = ireader.leaves().get(0).reader().getSortedDocValues("field" + i);
                    assertEquals(i, fieldN.nextDoc());
                    assertEquals(0, fieldN.ordValue());
                    BytesRef scratch = fieldN.lookupOrd(0);
                    assertEquals("value" + i, scratch.utf8ToString());
                    assertEquals(DocIdSetIterator.NO_MORE_DOCS, fieldN.nextDoc());
                }
            }
        }
    }

    public void testSortedSetDocValuesSingleUniqueValue() throws IOException {
        try (Directory directory = newDirectory()) {
            Analyzer analyzer = new MockAnalyzer(random());
            IndexWriterConfig conf = newIndexWriterConfig(analyzer);
            conf.setMergePolicy(newLogMergePolicy());
            try (RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, conf)) {
                for (int i = 0; i < NUM_DOCS; i++) {
                    Document doc = new Document();
                    doc.add(new SortedSetDocValuesField("field", newBytesRef("value")));
                    doc.add(new SortedSetDocValuesField("field" + i, newBytesRef("value" + i)));
                    iwriter.addDocument(doc);
                }
                iwriter.forceMerge(1);
            }

            try (IndexReader ireader = maybeWrapWithMergingReader(DirectoryReader.open(directory))) {
                assert ireader.leaves().size() == 1;
                var field = ireader.leaves().get(0).reader().getSortedSetDocValues("field");
                for (int i = 0; i < NUM_DOCS; i++) {
                    assertEquals(i, field.nextDoc());
                    assertEquals(1, field.docValueCount());
                    assertEquals(0, field.nextOrd());
                    BytesRef scratch = field.lookupOrd(0);
                    assertEquals("value", scratch.utf8ToString());
                    assertEquals(SortedSetDocValues.NO_MORE_ORDS, field.nextOrd());
                }
                assertEquals(DocIdSetIterator.NO_MORE_DOCS, field.nextDoc());
                for (int i = 0; i < NUM_DOCS; i++) {
                    var fieldN = ireader.leaves().get(0).reader().getSortedSetDocValues("field" + i);
                    assertEquals(i, fieldN.nextDoc());
                    assertEquals(1, fieldN.docValueCount());
                    assertEquals(0, fieldN.nextOrd());
                    BytesRef scratch = fieldN.lookupOrd(0);
                    assertEquals("value" + i, scratch.utf8ToString());
                    assertEquals(DocIdSetIterator.NO_MORE_DOCS, fieldN.nextDoc());
                    assertEquals(SortedSetDocValues.NO_MORE_ORDS, fieldN.nextOrd());
                }
            }
        }
    }

}
