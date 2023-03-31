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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class TopDocsCollectorContextTests extends ESTestCase {

    public void testShortcutTotalHitCountTextFieldExists() throws IOException {
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
                int hitCount = TopDocsCollectorContext.shortcutTotalHitCount(reader, testQuery, SearchContext.DEFAULT_TERMINATE_AFTER);
                assertEquals(-1, hitCount);
            }
        }
    }

    public void testShortcutTotalHitCountStringFieldExists() throws IOException {
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
                int hitCount = TopDocsCollectorContext.shortcutTotalHitCount(reader, testQuery, SearchContext.DEFAULT_TERMINATE_AFTER);
                assertEquals(2, hitCount);
            }
        }
    }

    public void testShortcutTotalHitCountStringFieldExistsTerminateAfter() throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {
            int numDocs = randomIntBetween(5, 10);
            for (int i = 0; i < numDocs; i++) {
                Document doc = new Document();
                doc.add(new StringField("string", "value", Field.Store.NO));
                doc.add(new SortedDocValuesField("string", new BytesRef("value")));
                iw.addDocument(doc);
            }
            iw.commit();
            try (IndexReader reader = iw.getReader()) {
                final Query testQuery = new FieldExistsQuery("string");
                assertEquals(hitCountUpTo(reader, 3), TopDocsCollectorContext.shortcutTotalHitCount(reader, testQuery, 3));
            }
        }
    }

    public void testShortcutTotalHitCountNumericFieldExists() throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {
            Document doc = new Document();
            doc.add(new IntPoint("int", 10));
            doc.add(new NumericDocValuesField("int", 10));
            iw.addDocument(doc);
            iw.addDocument(new Document());
            iw.commit();
            try (IndexReader reader = iw.getReader()) {
                final Query testQuery = new FieldExistsQuery("int");
                int hitCount = TopDocsCollectorContext.shortcutTotalHitCount(reader, testQuery, SearchContext.DEFAULT_TERMINATE_AFTER);
                assertEquals(1, hitCount);
            }
        }
    }

    public void testShortcutTotalHitCountMatchAllQuery() throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {
            int numDocs = randomIntBetween(5, 10);
            for (int i = 0; i < numDocs; i++) {
                Document doc = new Document();
                iw.addDocument(doc);
            }
            iw.commit();
            try (IndexReader reader = iw.getReader()) {
                final Query testQuery = new MatchAllDocsQuery();
                assertEquals(numDocs, TopDocsCollectorContext.shortcutTotalHitCount(reader, testQuery, 0));
                assertEquals(hitCountUpTo(reader, 3), TopDocsCollectorContext.shortcutTotalHitCount(reader, testQuery, 3));
            }
        }
    }

    public void testShortcutTotalHitCountTermQuery() throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {
            int numDocs = randomIntBetween(5, 10);
            for (int i = 0; i < numDocs; i++) {
                Document doc = new Document();
                doc.add(new StringField("string", "value", Field.Store.NO));
                iw.addDocument(doc);
            }
            iw.addDocument(new Document());
            iw.commit();
            try (IndexReader reader = iw.getReader()) {
                final Query testQuery = new TermQuery(new Term("string", "value"));
                assertEquals(numDocs, TopDocsCollectorContext.shortcutTotalHitCount(reader, testQuery, 0));
            }
        }
    }

    public void testShortcutTotalHitCountTermQueryTerminateAfter() throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {
            int numDocs = randomIntBetween(5, 10);
            for (int i = 0; i < numDocs; i++) {
                Document doc = new Document();
                doc.add(new StringField("string", "value", Field.Store.NO));
                iw.addDocument(doc);
            }
            iw.commit();
            try (IndexReader reader = iw.getReader()) {
                final Query testQuery = new TermQuery(new Term("string", "value"));
                assertEquals(hitCountUpTo(reader, 5), TopDocsCollectorContext.shortcutTotalHitCount(reader, testQuery, 5));
            }
        }
    }

    static int hitCountUpTo(IndexReader reader, int terminateAfter) {
        int total = 0;
        for (LeafReaderContext leaf : reader.leaves()) {
            total += leaf.reader().numDocs();
            if (total >= terminateAfter) {
                break;
            }
        }
        return total;
    }
}
