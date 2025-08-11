/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.mapper;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LatLonShape;
import org.apache.lucene.document.ShapeField;
import org.apache.lucene.geo.GeoTestUtil;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.geo.Point;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.search.CheckHits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryUtils;
import org.apache.lucene.store.Directory;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

public class LatLonShapeDocValuesQueryTests extends ESTestCase {

    private static final String FIELD_NAME = "field";

    public void testEqualsAndHashcode() {
        Polygon polygon = GeoTestUtil.nextPolygon();
        Query q1 = new LatLonShapeDocValuesQuery(FIELD_NAME, ShapeField.QueryRelation.INTERSECTS, polygon);
        Query q2 = new LatLonShapeDocValuesQuery(FIELD_NAME, ShapeField.QueryRelation.INTERSECTS, polygon);
        QueryUtils.checkEqual(q1, q2);

        Query q3 = new LatLonShapeDocValuesQuery(FIELD_NAME + "x", ShapeField.QueryRelation.INTERSECTS, polygon);
        QueryUtils.checkUnequal(q1, q3);

        Rectangle rectangle = GeoTestUtil.nextBox();
        Query q4 = new LatLonShapeDocValuesQuery(FIELD_NAME, ShapeField.QueryRelation.INTERSECTS, rectangle);
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
        GeoShapeIndexer indexer = new GeoShapeIndexer(true, FIELD_NAME);
        for (int id = 0; id < numDocs; id++) {
            Document doc = new Document();
            @SuppressWarnings("unchecked")
            Function<Boolean, Geometry> geometryFunc = ESTestCase.randomFrom(
                GeometryTestUtils::randomLine,
                GeometryTestUtils::randomPoint,
                GeometryTestUtils::randomPolygon
            );
            Geometry geometry = geometryFunc.apply(false);
            List<IndexableField> fields = indexer.indexShape(geometry);
            for (IndexableField field : fields) {
                doc.add(field);
            }
            BinaryGeoShapeDocValuesField docVal = new BinaryGeoShapeDocValuesField(FIELD_NAME);
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
            LatLonGeometry[] geometries = randomLuceneQueryGeometries();
            for (ShapeField.QueryRelation relation : ShapeField.QueryRelation.values()) {
                Query indexQuery = LatLonShape.newGeometryQuery(FIELD_NAME, relation, geometries);
                Query docValQuery = new LatLonShapeDocValuesQuery(FIELD_NAME, relation, geometries);
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
        GeoShapeIndexer indexer = new GeoShapeIndexer(true, FIELD_NAME);
        for (int id = 0; id < numDocs; id++) {
            Document doc = new Document();
            Geometry geometry = GeometryTestUtils.randomGeometryWithoutCircle(randomIntBetween(1, 5), false);
            List<IndexableField> fields = indexer.indexShape(geometry);
            for (IndexableField field : fields) {
                doc.add(field);
            }
            BinaryGeoShapeDocValuesField docVal = new BinaryGeoShapeDocValuesField(FIELD_NAME);
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
            LatLonGeometry[] geometries = randomLuceneQueryGeometries();
            for (ShapeField.QueryRelation relation : ShapeField.QueryRelation.values()) {
                if (relation == ShapeField.QueryRelation.CONTAINS) {
                    // We don't check this here as it might fail due to LUCENE-10514
                    continue;
                }
                Query indexQuery = LatLonShape.newGeometryQuery(FIELD_NAME, relation, geometries);
                Query docValQuery = new LatLonShapeDocValuesQuery(FIELD_NAME, relation, geometries);
                assertQueries(s, indexQuery, docValQuery, numDocs);
            }
        }
        IOUtils.close(r, dir);
    }

    private void assertQueries(IndexSearcher s, Query indexQuery, Query docValQuery, int numDocs) throws IOException {
        assertEquals(s.count(indexQuery), s.count(docValQuery));
        CheckHits.checkEqual(docValQuery, s.search(indexQuery, numDocs).scoreDocs, s.search(docValQuery, numDocs).scoreDocs);
    }

    private LatLonGeometry[] randomLuceneQueryGeometries() {
        int numGeom = randomIntBetween(1, 3);
        LatLonGeometry[] geometries = new LatLonGeometry[numGeom];
        for (int i = 0; i < numGeom; i++) {
            geometries[i] = randomLuceneQueryGeometry();
        }
        return geometries;
    }

    private LatLonGeometry randomLuceneQueryGeometry() {
        switch (randomInt(3)) {
            case 0:
                return GeoTestUtil.nextPolygon();
            case 1:
                return GeoTestUtil.nextCircle();
            case 2:
                return new Point(GeoTestUtil.nextLatitude(), GeoTestUtil.nextLongitude());
            default:
                return GeoTestUtil.nextBox();
        }
    }
}
