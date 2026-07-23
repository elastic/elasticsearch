/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query.bitmapterms;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.search.QueryUtils;
import org.elasticsearch.test.ESTestCase;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class IntBitmapIndexBKDQueryTests extends ESTestCase {

    private static final String FIELD = "f";

    public void testEqualsAndHashCode() {
        RoaringBitmap bm1 = RoaringBitmap.bitmapOf(1, 2, 3);
        RoaringBitmap bm2 = RoaringBitmap.bitmapOf(1, 2, 3);
        RoaringBitmap bm3 = RoaringBitmap.bitmapOf(1, 2);
        QueryUtils.check(new IntBitmapIndexBKDQuery(FIELD, bm1));
        QueryUtils.checkEqual(new IntBitmapIndexBKDQuery(FIELD, bm1), new IntBitmapIndexBKDQuery(FIELD, bm2));
        QueryUtils.checkUnequal(new IntBitmapIndexBKDQuery(FIELD, bm1), new IntBitmapIndexBKDQuery(FIELD, bm3));
        QueryUtils.checkUnequal(new IntBitmapIndexBKDQuery(FIELD, bm1), new IntBitmapIndexBKDQuery("g", bm1));
    }

    public void testToString() {
        RoaringBitmap empty = new RoaringBitmap();
        assertThat(new IntBitmapIndexBKDQuery(FIELD, empty).toString(FIELD), containsString("cardinality=0"));

        RoaringBitmap bitmap = RoaringBitmap.bitmapOf(1, 100, 1000);
        String str = new IntBitmapIndexBKDQuery(FIELD, bitmap).toString(FIELD);
        assertThat(str, containsString("cardinality=3"));
        assertThat(str, containsString("first=1"));
        assertThat(str, containsString("last=1000"));
    }

    public void testEmptyBitmap() throws IOException {
        Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir);
        Document doc = new Document();
        doc.add(new IntPoint(FIELD, 1));
        w.addDocument(doc);
        IndexReader reader = w.getReader();
        IndexSearcher searcher = newSearcher(reader);
        assertThat(searcher.count(new IntBitmapIndexBKDQuery(FIELD, new RoaringBitmap())), equalTo(0));
        w.close();
        reader.close();
        dir.close();
    }

    public void testNoIndexedField() throws IOException {
        Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir);
        w.addDocument(new Document());
        IndexReader reader = w.getReader();
        IndexSearcher searcher = newSearcher(reader);
        assertThat(searcher.count(new IntBitmapIndexBKDQuery(FIELD, RoaringBitmap.bitmapOf(1))), equalTo(0));
        w.close();
        reader.close();
        dir.close();
    }

    public void testSearch() throws IOException {
        Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir);
        for (int i = 0; i < 10; i++) {
            Document doc = new Document();
            doc.add(new IntPoint(FIELD, i));
            w.addDocument(doc);
        }
        // One doc with no field value
        w.addDocument(new Document());
        IndexReader reader = w.getReader();
        IndexSearcher searcher = newSearcher(reader);

        assertThat(searcher.count(new IntBitmapIndexBKDQuery(FIELD, RoaringBitmap.bitmapOf(1, 3, 5))), equalTo(3));
        assertThat(searcher.count(new IntBitmapIndexBKDQuery(FIELD, RoaringBitmap.bitmapOf(100))), equalTo(0));
        assertThat(searcher.count(new IntBitmapIndexBKDQuery(FIELD, RoaringBitmap.bitmapOf(0, 5, 9))), equalTo(3));

        w.close();
        reader.close();
        dir.close();
    }

    public void testMultipleSegments() throws IOException {
        Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir);
        Document doc = new Document();
        doc.add(new IntPoint(FIELD, 1));
        w.addDocument(doc);
        w.commit();
        doc = new Document();
        doc.add(new IntPoint(FIELD, 5));
        w.addDocument(doc);
        w.commit();
        doc = new Document();
        doc.add(new IntPoint(FIELD, 10));
        w.addDocument(doc);
        IndexReader reader = w.getReader();
        IndexSearcher searcher = newSearcher(reader);

        assertThat(searcher.count(new IntBitmapIndexBKDQuery(FIELD, RoaringBitmap.bitmapOf(1, 5, 10))), equalTo(3));
        assertThat(searcher.count(new IntBitmapIndexBKDQuery(FIELD, RoaringBitmap.bitmapOf(1, 3))), equalTo(1));

        w.close();
        reader.close();
        dir.close();
    }

    /**
     * Tests the CELL_INSIDE_QUERY path in MergePointVisitor: when many docs share a single value,
     * BKD creates a leaf cell whose min and max are both that value.
     */
    public void testManyDocsWithSameValue() throws IOException {
        Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir);
        for (int i = 0; i < 600; i++) {
            Document doc = new Document();
            doc.add(new IntPoint(FIELD, 42));
            w.addDocument(doc);
        }
        IndexReader reader = w.getReader();
        IndexSearcher searcher = newSearcher(reader);
        assertThat(searcher.count(new IntBitmapIndexBKDQuery(FIELD, RoaringBitmap.bitmapOf(42))), equalTo(600));
        assertThat(searcher.count(new IntBitmapIndexBKDQuery(FIELD, RoaringBitmap.bitmapOf(1))), equalTo(0));
        w.close();
        reader.close();
        dir.close();
    }

    public void testRangeSkipping() throws IOException {
        Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir);
        // Index values 1-100
        for (int i = 1; i <= 100; i++) {
            Document doc = new Document();
            doc.add(new IntPoint(FIELD, i));
            w.addDocument(doc);
        }
        IndexReader reader = w.getReader();
        IndexSearcher searcher = newSearcher(reader);

        // Bitmap entirely above the indexed range: no match
        assertThat(searcher.count(new IntBitmapIndexBKDQuery(FIELD, RoaringBitmap.bitmapOf(200, 300))), equalTo(0));
        // Bitmap entirely below the indexed range: no match
        assertThat(searcher.count(new IntBitmapIndexBKDQuery(FIELD, RoaringBitmap.bitmapOf(0))), equalTo(0));

        w.close();
        reader.close();
        dir.close();
    }
}
