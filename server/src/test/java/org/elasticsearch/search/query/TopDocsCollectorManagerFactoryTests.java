/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.query;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class TopDocsCollectorManagerFactoryTests extends ESTestCase {

    public void testShortcutTotalHitCountTextField() throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {
            Document doc = new Document();
            doc.add(new TextField("text", "value", Field.Store.NO));
            iw.addDocument(doc);
            doc = new Document();
            doc.add(new TextField("text", "", Field.Store.NO));
            iw.addDocument(doc);
            iw.addDocument(new Document());
            iw.commit();
            try (IndexReader reader = iw.getReader()) {
                final Query testQuery = new FieldExistsQuery("text");
                int hitCount = TopDocsCollectorManagerFactory.shortcutTotalHitCount(reader, testQuery);
                assertEquals(-1, hitCount);
            }
        }
    }

    public void testShortcutTotalHitCountStringField() throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {
            Document doc = new Document();
            doc.add(new StringField("string", "value", Field.Store.NO));
            doc.add(new SortedDocValuesField("string", new BytesRef("value")));
            iw.addDocument(doc);
            doc = new Document();
            doc.add(new StringField("string", "", Field.Store.NO));
            doc.add(new SortedDocValuesField("string", new BytesRef("")));
            iw.addDocument(doc);
            iw.addDocument(new Document());
            iw.commit();
            try (IndexReader reader = iw.getReader()) {
                final Query testQuery = new FieldExistsQuery("string");
                int hitCount = TopDocsCollectorManagerFactory.shortcutTotalHitCount(reader, testQuery);
                assertEquals(2, hitCount);
            }
        }
    }

    public void testShortcutTotalHitCountNumericField() throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {
            Document doc = new Document();
            doc.add(new IntPoint("int", 10));
            doc.add(new NumericDocValuesField("int", 10));
            iw.addDocument(doc);
            iw.addDocument(new Document());
            iw.commit();
            try (IndexReader reader = iw.getReader()) {
                final Query testQuery = new FieldExistsQuery("int");
                int hitCount = TopDocsCollectorManagerFactory.shortcutTotalHitCount(reader, testQuery);
                assertEquals(1, hitCount);
            }
        }
    }
}
