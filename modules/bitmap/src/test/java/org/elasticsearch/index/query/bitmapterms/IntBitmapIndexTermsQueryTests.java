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
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.search.QueryUtils;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.test.ESTestCase;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class IntBitmapIndexTermsQueryTests extends ESTestCase {

    private static final String FIELD = "f";
    private static final FieldType INDEX_TERMS_TYPE;

    static {
        INDEX_TERMS_TYPE = new FieldType();
        INDEX_TERMS_TYPE.setIndexOptions(IndexOptions.DOCS);
        INDEX_TERMS_TYPE.setOmitNorms(true);
        INDEX_TERMS_TYPE.setTokenized(false);
        INDEX_TERMS_TYPE.freeze();
    }

    private static void addField(Document doc, String field, int value) {
        byte[] bytes = new byte[Integer.BYTES];
        NumericUtils.intToSortableBytes(value, bytes, 0);
        doc.add(new Field(field, new BytesRef(bytes), INDEX_TERMS_TYPE));
    }

    public void testEqualsAndHashCode() {
        RoaringBitmap bm1 = RoaringBitmap.bitmapOf(1, 2, 3);
        RoaringBitmap bm2 = RoaringBitmap.bitmapOf(1, 2, 3);
        RoaringBitmap bm3 = RoaringBitmap.bitmapOf(1, 2);
        QueryUtils.check(new IntBitmapIndexTermsQuery(FIELD, bm1));
        QueryUtils.checkEqual(new IntBitmapIndexTermsQuery(FIELD, bm1), new IntBitmapIndexTermsQuery(FIELD, bm2));
        QueryUtils.checkUnequal(new IntBitmapIndexTermsQuery(FIELD, bm1), new IntBitmapIndexTermsQuery(FIELD, bm3));
        QueryUtils.checkUnequal(new IntBitmapIndexTermsQuery(FIELD, bm1), new IntBitmapIndexTermsQuery("g", bm1));
    }

    public void testToString() {
        RoaringBitmap empty = new RoaringBitmap();
        assertThat(new IntBitmapIndexTermsQuery(FIELD, empty).toString(FIELD), containsString("cardinality=0"));

        RoaringBitmap bitmap = RoaringBitmap.bitmapOf(1, 100, 1000);
        String str = new IntBitmapIndexTermsQuery(FIELD, bitmap).toString(FIELD);
        assertThat(str, containsString("cardinality=3"));
        assertThat(str, containsString("first=1"));
        assertThat(str, containsString("last=1000"));
    }

    public void testEmptyBitmap() throws IOException {
        Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir);
        Document doc = new Document();
        addField(doc, FIELD, 1);
        w.addDocument(doc);
        IndexReader reader = w.getReader();
        IndexSearcher searcher = newSearcher(reader);
        assertThat(searcher.count(new IntBitmapIndexTermsQuery(FIELD, new RoaringBitmap())), equalTo(0));
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
        assertThat(searcher.count(new IntBitmapIndexTermsQuery(FIELD, RoaringBitmap.bitmapOf(1))), equalTo(0));
        w.close();
        reader.close();
        dir.close();
    }

    public void testSearch() throws IOException {
        Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir);
        for (int i = 0; i < 10; i++) {
            Document doc = new Document();
            addField(doc, FIELD, i);
            w.addDocument(doc);
        }
        // One doc with no field value
        w.addDocument(new Document());
        IndexReader reader = w.getReader();
        IndexSearcher searcher = newSearcher(reader);

        assertThat(searcher.count(new IntBitmapIndexTermsQuery(FIELD, RoaringBitmap.bitmapOf(1, 3, 5))), equalTo(3));
        assertThat(searcher.count(new IntBitmapIndexTermsQuery(FIELD, RoaringBitmap.bitmapOf(100))), equalTo(0));
        assertThat(searcher.count(new IntBitmapIndexTermsQuery(FIELD, RoaringBitmap.bitmapOf(0, 5, 9))), equalTo(3));

        w.close();
        reader.close();
        dir.close();
    }

    public void testMultipleSegments() throws IOException {
        Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir);
        Document doc = new Document();
        addField(doc, FIELD, 1);
        w.addDocument(doc);
        w.commit();
        doc = new Document();
        addField(doc, FIELD, 5);
        w.addDocument(doc);
        w.commit();
        doc = new Document();
        addField(doc, FIELD, 10);
        w.addDocument(doc);
        IndexReader reader = w.getReader();
        IndexSearcher searcher = newSearcher(reader);

        assertThat(searcher.count(new IntBitmapIndexTermsQuery(FIELD, RoaringBitmap.bitmapOf(1, 5, 10))), equalTo(3));
        assertThat(searcher.count(new IntBitmapIndexTermsQuery(FIELD, RoaringBitmap.bitmapOf(1, 3))), equalTo(1));

        w.close();
        reader.close();
        dir.close();
    }

    /** Verifies the merge-scan skips forward correctly when the bitmap leads the terms enum. */
    public void testBitmapLeadsTerm() throws IOException {
        Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir);
        // Index only even values 0, 2, 4, 6, 8
        for (int i = 0; i <= 8; i += 2) {
            Document doc = new Document();
            addField(doc, FIELD, i);
            w.addDocument(doc);
        }
        IndexReader reader = w.getReader();
        IndexSearcher searcher = newSearcher(reader);

        // Bitmap has odd values 1, 3, 5, 7 — none indexed
        assertThat(searcher.count(new IntBitmapIndexTermsQuery(FIELD, RoaringBitmap.bitmapOf(1, 3, 5, 7))), equalTo(0));
        // Bitmap has mix of indexed (0, 4) and non-indexed (1, 3)
        assertThat(searcher.count(new IntBitmapIndexTermsQuery(FIELD, RoaringBitmap.bitmapOf(0, 1, 3, 4))), equalTo(2));

        w.close();
        reader.close();
        dir.close();
    }

    /** Verifies the merge-scan skips forward correctly when the terms enum leads the bitmap. */
    public void testTermsEnumLeadsBitmap() throws IOException {
        Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir);
        // Index 0..9
        for (int i = 0; i < 10; i++) {
            Document doc = new Document();
            addField(doc, FIELD, i);
            w.addDocument(doc);
        }
        IndexReader reader = w.getReader();
        IndexSearcher searcher = newSearcher(reader);

        // Bitmap starts high, so terms enum must seek past its lower terms
        assertThat(searcher.count(new IntBitmapIndexTermsQuery(FIELD, RoaringBitmap.bitmapOf(7, 8, 9))), equalTo(3));
        // Bitmap has a single value that's in the middle of the terms
        assertThat(searcher.count(new IntBitmapIndexTermsQuery(FIELD, RoaringBitmap.bitmapOf(5))), equalTo(1));

        w.close();
        reader.close();
        dir.close();
    }
}
