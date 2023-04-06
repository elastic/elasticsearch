/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.mapper;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.ShapeField;
import org.apache.lucene.document.XYShape;
import org.apache.lucene.geo.XShapeTestUtil;
import org.apache.lucene.geo.XYGeometry;
import org.apache.lucene.geo.XYPolygon;
import org.apache.lucene.geo.XYRectangle;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.search.CheckHits;
import org.apache.lucene.tests.search.QueryUtils;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.spatial.common.ShapeUtils;
import org.elasticsearch.xpack.spatial.index.fielddata.CoordinateEncoder;
import org.elasticsearch.xpack.spatial.util.ShapeTestUtils;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

public class CartesianShapeDocValuesQueryTests extends ESTestCase {

    private static final String FIELD_NAME = "field";

    public void testEqualsAndHashcode() {
        XYPolygon polygon = ShapeUtils.toLuceneXYPolygon(ShapeTestUtils.randomPolygon(false));
        Query q1 = new CartesianShapeDocValuesQuery(FIELD_NAME, ShapeField.QueryRelation.INTERSECTS, polygon);
        Query q2 = new CartesianShapeDocValuesQuery(FIELD_NAME, ShapeField.QueryRelation.INTERSECTS, polygon);
        QueryUtils.checkEqual(q1, q2);

        Query q3 = new CartesianShapeDocValuesQuery(FIELD_NAME + "x", ShapeField.QueryRelation.INTERSECTS, polygon);
        QueryUtils.checkUnequal(q1, q3);

        XYRectangle rectangle = XShapeTestUtil.nextBox();
        Query q4 = new CartesianShapeDocValuesQuery(FIELD_NAME, ShapeField.QueryRelation.INTERSECTS, rectangle);
        QueryUtils.checkUnequal(q1, q4);
    }

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
        CartesianShapeIndexer indexer = new CartesianShapeIndexer(FIELD_NAME);
        for (int id = 0; id < numDocs; id++) {
            Document doc = new Document();
            @SuppressWarnings("unchecked")
            Function<Boolean, Geometry> geometryFunc = ESTestCase.randomFrom(
                ShapeTestUtils::randomLine,
                ShapeTestUtils::randomPoint,
                ShapeTestUtils::randomPolygon
            );
            Geometry geometry = geometryFunc.apply(false);
            List<IndexableField> fields = indexer.indexShape(geometry);
            for (IndexableField field : fields) {
                doc.add(field);
            }
            BinaryShapeDocValuesField docVal = new BinaryShapeDocValuesField(FIELD_NAME, CoordinateEncoder.GEO);
            docVal.add(fields, geometry);
            doc.add(docVal);
            w.addDocument(doc);
        }

        if (random().nextBoolean()) {
            w.forceMerge(1);
        }
        final IndexReader r = DirectoryReader.open(w);
        w.close();

        IndexSearcher s = newSearcher(r);
        for (int i = 0; i < 25; i++) {
            XYGeometry[] geometries = randomLuceneQueryGeometries();
            for (ShapeField.QueryRelation relation : ShapeField.QueryRelation.values()) {
                Query indexQuery = XYShape.newGeometryQuery(FIELD_NAME, relation, geometries);
                Query docValQuery = new CartesianShapeDocValuesQuery(FIELD_NAME, relation, geometries);
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
            Geometry geometry = ShapeTestUtils.randomGeometryWithoutCircle(randomIntBetween(1, 5), false);
            List<IndexableField> fields = indexer.indexShape(geometry);
            for (IndexableField field : fields) {
                doc.add(field);
            }
            BinaryShapeDocValuesField docVal = new BinaryShapeDocValuesField(FIELD_NAME, CoordinateEncoder.GEO);
            docVal.add(fields, geometry);
            doc.add(docVal);
            w.addDocument(doc);
        }

        if (random().nextBoolean()) {
            w.forceMerge(1);
        }
        final IndexReader r = DirectoryReader.open(w);
        w.close();

        IndexSearcher s = newSearcher(r);
        for (int i = 0; i < 25; i++) {
            XYGeometry[] geometries = randomLuceneQueryGeometries();
            for (ShapeField.QueryRelation relation : ShapeField.QueryRelation.values()) {
                Query indexQuery = XYShape.newGeometryQuery(FIELD_NAME, relation, geometries);
                Query docValQuery = new CartesianShapeDocValuesQuery(FIELD_NAME, relation, geometries);
                assertQueries(s, indexQuery, docValQuery, numDocs);
            }
        }
        IOUtils.close(r, dir);
    }

    private void assertQueries(IndexSearcher s, Query indexQuery, Query docValQuery, int numDocs) throws IOException {
        assertEquals(s.count(indexQuery), s.count(docValQuery));
        CheckHits.checkEqual(docValQuery, s.search(indexQuery, numDocs).scoreDocs, s.search(docValQuery, numDocs).scoreDocs);
    }

    private XYGeometry[] randomLuceneQueryGeometries() {
        int numGeom = randomIntBetween(1, 3);
        XYGeometry[] geometries = new XYGeometry[numGeom];
        for (int i = 0; i < numGeom; i++) {
            geometries[i] = randomLuceneQueryGeometry();
        }
        return geometries;
    }

    private XYGeometry randomLuceneQueryGeometry() {
        return switch (randomInt(3)) {
            case 0 -> ShapeUtils.toLuceneXYPolygon(ShapeTestUtils.randomPolygon(false));
            case 1 -> ShapeUtils.toLuceneXYCircle(ShapeTestUtils.randomCircle(false));
            case 2 -> ShapeUtils.toLuceneXYPoint(ShapeTestUtils.randomPoint(false));
            default -> XShapeTestUtil.nextBox();
        };
    }
}
