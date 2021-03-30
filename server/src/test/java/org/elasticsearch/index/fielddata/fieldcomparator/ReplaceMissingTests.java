/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata.fieldcomparator;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

public class ReplaceMissingTests extends ESTestCase {

    public void test() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig(null);
        iwc.setMergePolicy(newLogMergePolicy());
        IndexWriter iw = new IndexWriter(dir, iwc);

        Document doc = new Document();
        doc.add(new SortedDocValuesField("field", new BytesRef("cat")));
        iw.addDocument(doc);

        doc = new Document();
        iw.addDocument(doc);

        doc = new Document();
        doc.add(new SortedDocValuesField("field", new BytesRef("dog")));
        iw.addDocument(doc);
        iw.forceMerge(1);
        iw.close();

        DirectoryReader reader = DirectoryReader.open(dir);
        LeafReader ar = getOnlyLeafReader(reader);
        SortedDocValues raw = ar.getSortedDocValues("field");
        assertEquals(2, raw.getValueCount());

        // existing values
        SortedDocValues dv = new BytesRefFieldComparatorSource.ReplaceMissing(raw, new BytesRef("cat"));
        assertEquals(2, dv.getValueCount());
        assertEquals("cat", dv.lookupOrd(0).utf8ToString());
        assertEquals("dog", dv.lookupOrd(1).utf8ToString());

        assertTrue(dv.advanceExact(0));
        assertEquals(0, dv.ordValue());
        assertTrue(dv.advanceExact(1));
        assertEquals(0, dv.ordValue());
        assertTrue(dv.advanceExact(2));
        assertEquals(1, dv.ordValue());

        raw = ar.getSortedDocValues("field");
        dv = new BytesRefFieldComparatorSource.ReplaceMissing(raw, new BytesRef("dog"));
        assertEquals(2, dv.getValueCount());
        assertEquals("cat", dv.lookupOrd(0).utf8ToString());
        assertEquals("dog", dv.lookupOrd(1).utf8ToString());

        assertTrue(dv.advanceExact(0));
        assertEquals(0, dv.ordValue());
        assertTrue(dv.advanceExact(1));
        assertEquals(1, dv.ordValue());
        assertTrue(dv.advanceExact(2));
        assertEquals(1, dv.ordValue());

        // non-existing values
        raw = ar.getSortedDocValues("field");
        dv = new BytesRefFieldComparatorSource.ReplaceMissing(raw, new BytesRef("apple"));
        assertEquals(3, dv.getValueCount());
        assertEquals("apple", dv.lookupOrd(0).utf8ToString());
        assertEquals("cat", dv.lookupOrd(1).utf8ToString());
        assertEquals("dog", dv.lookupOrd(2).utf8ToString());

        assertTrue(dv.advanceExact(0));
        assertEquals(1, dv.ordValue());
        assertTrue(dv.advanceExact(1));
        assertEquals(0, dv.ordValue());
        assertTrue(dv.advanceExact(2));
        assertEquals(2, dv.ordValue());

        raw = ar.getSortedDocValues("field");
        dv = new BytesRefFieldComparatorSource.ReplaceMissing(raw, new BytesRef("company"));
        assertEquals(3, dv.getValueCount());
        assertEquals("cat", dv.lookupOrd(0).utf8ToString());
        assertEquals("company", dv.lookupOrd(1).utf8ToString());
        assertEquals("dog", dv.lookupOrd(2).utf8ToString());

        assertTrue(dv.advanceExact(0));
        assertEquals(0, dv.ordValue());
        assertTrue(dv.advanceExact(1));
        assertEquals(1, dv.ordValue());
        assertTrue(dv.advanceExact(2));
        assertEquals(2, dv.ordValue());

        raw = ar.getSortedDocValues("field");
        dv = new BytesRefFieldComparatorSource.ReplaceMissing(raw, new BytesRef("ebay"));
        assertEquals(3, dv.getValueCount());
        assertEquals("cat", dv.lookupOrd(0).utf8ToString());
        assertEquals("dog", dv.lookupOrd(1).utf8ToString());
        assertEquals("ebay", dv.lookupOrd(2).utf8ToString());

        assertTrue(dv.advanceExact(0));
        assertEquals(0, dv.ordValue());
        assertTrue(dv.advanceExact(1));
        assertEquals(2, dv.ordValue());
        assertTrue(dv.advanceExact(2));
        assertEquals(1, dv.ordValue());

        reader.close();
        dir.close();
    }
}
