/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues.Doubles;
import org.elasticsearch.index.fielddata.ScriptDocValues.DoublesSupplier;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.script.field.DelegateDocValuesField;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class HalfFloatFielddataTests extends ESTestCase {

    public void testSingleValued() throws IOException {
        Directory dir = newDirectory();
        // we need the default codec to check for singletons
        IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null).setCodec(TestUtil.getDefaultCodec()));
        LuceneDocument doc = new LuceneDocument();
        NumberFieldMapper.NumberType.HALF_FLOAT.addFields(doc, "half_float", 3f, false, true, false);
        w.addDocument(doc);
        final DirectoryReader dirReader = DirectoryReader.open(w);
        LeafReader reader = getOnlyLeafReader(dirReader);
        SortedNumericDoubleValues values = new SortedDoublesIndexFieldData.SortedNumericHalfFloatFieldData(
            reader,
            "half_float",
            (dv, n) -> new DelegateDocValuesField(new Doubles(new DoublesSupplier(dv)), n)
        ).getDoubleValues();
        assertNotNull(FieldData.unwrapSingleton(values));
        assertTrue(values.advanceExact(0));
        assertEquals(1, values.docValueCount());
        assertEquals(3f, values.nextValue(), 0f);
        IOUtils.close(dirReader, w, dir);
    }

    public void testMultiValued() throws IOException {
        Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null));
        LuceneDocument doc = new LuceneDocument();
        NumberFieldMapper.NumberType.HALF_FLOAT.addFields(doc, "half_float", 3f, false, true, false);
        NumberFieldMapper.NumberType.HALF_FLOAT.addFields(doc, "half_float", 2f, false, true, false);
        w.addDocument(doc);
        final DirectoryReader dirReader = DirectoryReader.open(w);
        LeafReader reader = getOnlyLeafReader(dirReader);
        SortedNumericDoubleValues values = new SortedDoublesIndexFieldData.SortedNumericHalfFloatFieldData(
            reader,
            "half_float",
            (dv, n) -> new DelegateDocValuesField(new Doubles(new DoublesSupplier(dv)), n)
        ).getDoubleValues();
        assertNull(FieldData.unwrapSingleton(values));
        assertTrue(values.advanceExact(0));
        assertEquals(2, values.docValueCount());
        assertEquals(2f, values.nextValue(), 0f);
        assertEquals(3f, values.nextValue(), 0f);
        IOUtils.close(dirReader, w, dir);
    }
}
