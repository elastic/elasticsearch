/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.lucene.spatial;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.XYDocValuesField;
import org.apache.lucene.document.XYPointField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.search.CheckHits;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.geo.ShapeTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class CartesianPointDocValuesQueryTests extends ESTestCase {

    private static final String FIELD_NAME = "field";

    public void testIndexSimpleShapes() throws Exception {
        IndexWriterConfig iwc = newIndexWriterConfig();
        // Else seeds may not reproduce:
        iwc.setMergeScheduler(new SerialMergeScheduler());
        // Else we can get O(N^2) merging:
        iwc.setMaxBufferedDocs(10);
        Directory dir = newDirectory();
        // RandomIndexWriter is too slow here:
        IndexWriter w = new IndexWriter(dir, iwc);
        final int numDocs = randomIntBetween(10, 1000);
        for (int id = 0; id < numDocs; id++) {
            Document doc = new Document();
            Point point = ShapeTestUtils.randomPoint();
            doc.add(new XYPointField(FIELD_NAME, (float) point.getX(), (float) point.getY()));
            doc.add(new XYDocValuesField(FIELD_NAME, (float) point.getX(), (float) point.getY()));
            w.addDocument(doc);
        }

        if (random().nextBoolean()) {
            w.forceMerge(1);
        }
        final IndexReader r = DirectoryReader.open(w);
        w.close();

        IndexSearcher s = newSearcher(r);
        for (int i = 0; i < 25; i++) {
            Geometry geometry = ShapeTestUtils.randomGeometry(false);
            for (ShapeRelation relation : ShapeRelation.values()) {
                Query indexQuery = XYQueriesUtils.toXYPointQuery(geometry, FIELD_NAME, relation, true, false);
                Query docValQuery = XYQueriesUtils.toXYPointQuery(geometry, FIELD_NAME, relation, false, true);
                assertQueries(s, indexQuery, docValQuery, numDocs);
            }
        }
        IOUtils.close(r, dir);
    }

    public void testIndexMultiShapes() throws Exception {
        IndexWriterConfig iwc = newIndexWriterConfig();
        // Else seeds may not reproduce:
        iwc.setMergeScheduler(new SerialMergeScheduler());
        // Else we can get O(N^2) merging:
        iwc.setMaxBufferedDocs(10);
        Directory dir = newDirectory();
        // RandomIndexWriter is too slow here:
        IndexWriter w = new IndexWriter(dir, iwc);
        final int numDocs = randomIntBetween(10, 100);
        CartesianShapeIndexer indexer = new CartesianShapeIndexer(FIELD_NAME);
        for (int id = 0; id < numDocs; id++) {
            Document doc = new Document();
            for (int i = 0; i < randomIntBetween(1, 5); i++) {
                Point point = ShapeTestUtils.randomPoint();
                doc.add(new XYPointField(FIELD_NAME, (float) point.getX(), (float) point.getY()));
                doc.add(new XYDocValuesField(FIELD_NAME, (float) point.getX(), (float) point.getY()));
                w.addDocument(doc);
            }
            w.addDocument(doc);
        }

        if (random().nextBoolean()) {
            w.forceMerge(1);
        }
        final IndexReader r = DirectoryReader.open(w);
        w.close();

        IndexSearcher s = newSearcher(r);
        for (int i = 0; i < 25; i++) {
            Geometry geometry = ShapeTestUtils.randomGeometry(false);
            for (ShapeRelation relation : ShapeRelation.values()) {
                Query indexQuery = XYQueriesUtils.toXYPointQuery(geometry, FIELD_NAME, relation, true, false);
                Query docValQuery = XYQueriesUtils.toXYPointQuery(geometry, FIELD_NAME, relation, false, true);
                assertQueries(s, indexQuery, docValQuery, numDocs);
            }
        }
        IOUtils.close(r, dir);
    }

    private void assertQueries(IndexSearcher s, Query indexQuery, Query docValQuery, int numDocs) throws IOException {
        assertEquals(s.count(indexQuery), s.count(docValQuery));
        CheckHits.checkEqual(docValQuery, s.search(indexQuery, numDocs).scoreDocs, s.search(docValQuery, numDocs).scoreDocs);
    }
}
