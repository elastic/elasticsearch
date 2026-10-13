/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.columnar;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class ColumNARDocValuesUpdatesTests extends ESTestCase {

    /** A binary doc-values update on a ColumNAR string column must be applied and read back. */
    public void testUpdateBinaryDocValue() throws IOException {
        try (Directory dir = newDirectory()) {
            IndexWriterConfig iwc = new IndexWriterConfig().setCodec(ColumnarTestUtils.columnarCodec(ColumnarFieldType.STRING));
            try (IndexWriter w = new IndexWriter(dir, iwc)) {
                for (int i = 0; i < 3; i++) {
                    Document d = new Document();
                    d.add(new StringField("id", "d" + i, Field.Store.NO));
                    d.add(new Field("value", ColumnarTestUtils.stringPayload("v" + i), ColumnarTestUtils.columnarBinaryFieldType()));
                    w.addDocument(d);
                }
                w.commit();

                w.updateBinaryDocValue(new Term("id", "d1"), "value", ColumnarTestUtils.stringPayload("updated-1"));
                w.commit();

                try (DirectoryReader r = DirectoryReader.open(dir)) {
                    assertEquals(1, r.leaves().size());
                    LeafReaderContext ctx = r.leaves().get(0);
                    BinaryDocValues bdv = ctx.reader().getBinaryDocValues("value");
                    for (int i = 0; i < 3; i++) {
                        assertEquals(i, bdv.nextDoc());
                        BytesRef expected = i == 1
                            ? ColumnarTestUtils.stringPayload("updated-1")
                            : ColumnarTestUtils.stringPayload("v" + i);
                        assertEquals("doc " + i, expected, bdv.binaryValue());
                    }
                    assertEquals(DocIdSetIterator.NO_MORE_DOCS, bdv.nextDoc());
                }
            }
        }
    }

    /**
     * Updates applied to different segments must survive a merge: the merged column is rebuilt via
     * {@code mergeBinaryField}, which also resolves the column type without the mapping.
     */
    public void testUpdateThenMergeAcrossSegments() throws IOException {
        try (Directory dir = newDirectory()) {
            IndexWriterConfig iwc = new IndexWriterConfig().setCodec(ColumnarTestUtils.columnarCodec(ColumnarFieldType.STRING));
            try (IndexWriter w = new IndexWriter(dir, iwc)) {
                for (int i = 0; i < 2; i++) {
                    Document d = new Document();
                    d.add(new StringField("id", "d" + i, Field.Store.NO));
                    d.add(new Field("value", ColumnarTestUtils.stringPayload("v" + i), ColumnarTestUtils.columnarBinaryFieldType()));
                    w.addDocument(d);
                }
                w.commit(); // segment 1
                for (int i = 2; i < 4; i++) {
                    Document d = new Document();
                    d.add(new StringField("id", "d" + i, Field.Store.NO));
                    d.add(new Field("value", ColumnarTestUtils.stringPayload("v" + i), ColumnarTestUtils.columnarBinaryFieldType()));
                    w.addDocument(d);
                }
                w.commit(); // segment 2

                // Update one doc in each segment, then merge the two segments into one.
                w.updateBinaryDocValue(new Term("id", "d0"), "value", ColumnarTestUtils.stringPayload("updated-0"));
                w.updateBinaryDocValue(new Term("id", "d3"), "value", ColumnarTestUtils.stringPayload("updated-3"));
                w.forceMerge(1);
                w.commit();

                try (DirectoryReader r = DirectoryReader.open(dir)) {
                    assertEquals(1, r.leaves().size());
                    BinaryDocValues bdv = r.leaves().get(0).reader().getBinaryDocValues("value");
                    String[] expected = { "updated-0", "v1", "v2", "updated-3" };
                    for (int i = 0; i < 4; i++) {
                        assertEquals(i, bdv.nextDoc());
                        assertEquals("doc " + i, ColumnarTestUtils.stringPayload(expected[i]), bdv.binaryValue());
                    }
                    assertEquals(DocIdSetIterator.NO_MORE_DOCS, bdv.nextDoc());
                }
            }
        }
    }
}
