/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.store.Directory;
import org.elasticsearch.core.internal.io.IOUtils;
import org.apache.lucene.util.TestUtil;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class HalfFloatFielddataTests extends ESTestCase {

    public void testSingleValued() throws IOException {
        Directory dir = newDirectory();
        // we need the default codec to check for singletons
        IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null).setCodec(TestUtil.getDefaultCodec()));
        Document doc = new Document();
        for (IndexableField f : NumberFieldMapper.NumberType.HALF_FLOAT.createFields("half_float", 3f, false, true, false)) {
            doc.add(f);
        }
        w.addDocument(doc);
        final DirectoryReader dirReader = DirectoryReader.open(w);
        LeafReader reader = getOnlyLeafReader(dirReader);
        SortedNumericDoubleValues values = new SortedNumericIndexFieldData.SortedNumericHalfFloatFieldData(
                reader, "half_float").getDoubleValues();
        assertNotNull(FieldData.unwrapSingleton(values));
        assertTrue(values.advanceExact(0));
        assertEquals(1, values.docValueCount());
        assertEquals(3f, values.nextValue(), 0f);
        IOUtils.close(dirReader, w, dir);
    }

    public void testMultiValued() throws IOException {
        Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null));
        Document doc = new Document();
        for (IndexableField f : NumberFieldMapper.NumberType.HALF_FLOAT.createFields("half_float", 3f, false, true, false)) {
            doc.add(f);
        }
        for (IndexableField f : NumberFieldMapper.NumberType.HALF_FLOAT.createFields("half_float", 2f, false, true, false)) {
            doc.add(f);
        }
        w.addDocument(doc);
        final DirectoryReader dirReader = DirectoryReader.open(w);
        LeafReader reader = getOnlyLeafReader(dirReader);
        SortedNumericDoubleValues values = new SortedNumericIndexFieldData.SortedNumericHalfFloatFieldData(
                reader, "half_float").getDoubleValues();
        assertNull(FieldData.unwrapSingleton(values));
        assertTrue(values.advanceExact(0));
        assertEquals(2, values.docValueCount());
        assertEquals(2f, values.nextValue(), 0f);
        assertEquals(3f, values.nextValue(), 0f);
        IOUtils.close(dirReader, w, dir);
    }
}
