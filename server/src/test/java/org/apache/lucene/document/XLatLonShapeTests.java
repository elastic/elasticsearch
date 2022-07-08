/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.document;

import org.apache.lucene.document.ShapeField.QueryRelation;
import org.apache.lucene.geo.Circle;
import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.geo.GeoTestUtil;
import org.apache.lucene.geo.GeoUtils;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.geo.Line;
import org.apache.lucene.geo.Point;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.geo.XTessellator;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.elasticsearch.core.internal.io.IOUtils;

import static org.apache.lucene.geo.GeoEncodingUtils.decodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.decodeLongitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitude;

/** Test case for indexing polygons and querying by bounding box */
public class XLatLonShapeTests extends LuceneTestCase {
    protected static String FIELDNAME = "field";

    /** quantizes a latitude value to be consistent with index encoding */
    protected static double quantizeLat(double rawLat) {
        return decodeLatitude(encodeLatitude(rawLat));
    }

    /** quantizes a longitude value to be consistent with index encoding */
    protected static double quantizeLon(double rawLon) {
        return decodeLongitude(encodeLongitude(rawLon));
    }

    protected void addPolygonsToDoc(String field, Document doc, Polygon polygon) {
        Field[] fields = XLatLonShape.createIndexableFields(field, polygon);
        for (Field f : fields) {
            doc.add(f);
        }
    }

    protected void addLineToDoc(String field, Document doc, Line line) {
        Field[] fields = LatLonShape.createIndexableFields(field, line);
        for (Field f : fields) {
            doc.add(f);
        }
    }

    protected Query newRectQuery(String field, double minLat, double maxLat, double minLon, double maxLon) {
        return LatLonShape.newBoxQuery(field, QueryRelation.INTERSECTS, minLat, maxLat, minLon, maxLon);
    }

    /** test we can search for a point with a standard number of vertices*/
    public void testBasicIntersects() throws Exception {
        int numVertices = TestUtil.nextInt(random(), 50, 100);
        Directory dir = newDirectory();
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

        // add a random polygon document
        Polygon p = GeoTestUtil.createRegularPolygon(0, 90, atLeast(1000000), numVertices);
        Document document = new Document();
        addPolygonsToDoc(FIELDNAME, document, p);
        writer.addDocument(document);

        // add a line document
        document = new Document();
        // add a line string
        double lats[] = new double[p.numPoints() - 1];
        double lons[] = new double[p.numPoints() - 1];
        for (int i = 0; i < lats.length; ++i) {
            lats[i] = p.getPolyLat(i);
            lons[i] = p.getPolyLon(i);
        }
        Line l = new Line(lats, lons);
        addLineToDoc(FIELDNAME, document, l);
        writer.addDocument(document);

        ////// search /////
        // search an intersecting bbox
        IndexReader reader = writer.getReader();
        writer.close();
        IndexSearcher searcher = newSearcher(reader);
        double minLat = Math.min(lats[0], lats[1]);
        double minLon = Math.min(lons[0], lons[1]);
        double maxLat = Math.max(lats[0], lats[1]);
        double maxLon = Math.max(lons[0], lons[1]);
        Query q = newRectQuery(FIELDNAME, minLat, maxLat, minLon, maxLon);
        assertEquals(2, searcher.count(q));

        // search a disjoint bbox
        q = newRectQuery(FIELDNAME, p.minLat - 1d, p.minLat + 1, p.minLon - 1d, p.minLon + 1d);
        assertEquals(0, searcher.count(q));

        IOUtils.close(reader, dir);
    }

    public void testBasicContains() throws Exception {
        Directory dir = newDirectory();
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

        // add a random polygon document
        double[] polyLats = new double[] { -10, -10, 10, 10, -10 };
        double[] polyLons = new double[] { -10, 10, 10, -10, -10 };
        Polygon p = new Polygon(polyLats, polyLons);
        Document document = new Document();
        addPolygonsToDoc(FIELDNAME, document, p);
        writer.addDocument(document);

        // add a line document
        document = new Document();
        // add a line string
        double lats[] = new double[p.numPoints() - 1];
        double lons[] = new double[p.numPoints() - 1];
        for (int i = 0; i < lats.length; ++i) {
            lats[i] = p.getPolyLat(i);
            lons[i] = p.getPolyLon(i);
        }
        Line l = new Line(lats, lons);
        addLineToDoc(FIELDNAME, document, l);
        writer.addDocument(document);

        ////// search /////
        // search a Polygon
        IndexReader reader = writer.getReader();
        writer.close();
        IndexSearcher searcher = newSearcher(reader);
        polyLats = new double[] { -5, -5, 5, 5, -5 };
        polyLons = new double[] { -5, 5, 5, -5, -5 };
        Polygon query = new Polygon(polyLats, polyLons);
        Query q = LatLonShape.newPolygonQuery(FIELDNAME, QueryRelation.CONTAINS, query);
        assertEquals(1, searcher.count(q));

        // search a bounding box
        searcher = newSearcher(reader);
        q = LatLonShape.newBoxQuery(FIELDNAME, QueryRelation.CONTAINS, 0, 0, 0, 0);
        assertEquals(1, searcher.count(q));
        IOUtils.close(reader, dir);
    }

    /** test random polygons with a single hole */
    public void testPolygonWithHole() throws Exception {
        int numVertices = TestUtil.nextInt(random(), 50, 100);
        Directory dir = newDirectory();
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

        // add a random polygon with a hole
        Polygon inner = new Polygon(new double[] { -1d, -1d, 1d, 1d, -1d }, new double[] { -91d, -89d, -89d, -91.0, -91.0 });
        Polygon outer = GeoTestUtil.createRegularPolygon(0, -90, atLeast(1000000), numVertices);

        Document document = new Document();
        addPolygonsToDoc(FIELDNAME, document, new Polygon(outer.getPolyLats(), outer.getPolyLons(), inner));
        writer.addDocument(document);

        ///// search //////
        IndexReader reader = writer.getReader();
        writer.close();
        IndexSearcher searcher = newSearcher(reader);

        // search a bbox in the hole
        Query q = newRectQuery(FIELDNAME, inner.minLat + 1e-6, inner.maxLat - 1e-6, inner.minLon + 1e-6, inner.maxLon - 1e-6);
        assertEquals(0, searcher.count(q));

        IOUtils.close(reader, dir);
    }

    /** test we can search for a point with a large number of vertices*/
    public void testLargeVertexPolygon() throws Exception {
        int numVertices = TEST_NIGHTLY ? TestUtil.nextInt(random(), 200000, 500000) : TestUtil.nextInt(random(), 20000, 50000);
        IndexWriterConfig iwc = newIndexWriterConfig();
        iwc.setMergeScheduler(new SerialMergeScheduler());
        int mbd = iwc.getMaxBufferedDocs();
        if (mbd != -1 && mbd < numVertices / 100) {
            iwc.setMaxBufferedDocs(numVertices / 100);
        }
        Directory dir = newFSDirectory(createTempDir(getClass().getSimpleName()));
        IndexWriter writer = new IndexWriter(dir, iwc);

        // add a random polygon without a hole
        Polygon p = GeoTestUtil.createRegularPolygon(0, 90, atLeast(1000000), numVertices);
        Document document = new Document();
        addPolygonsToDoc(FIELDNAME, document, p);
        writer.addDocument(document);

        // add a random polygon with a hole
        Polygon inner = new Polygon(new double[] { -1d, -1d, 1d, 1d, -1d }, new double[] { -91d, -89d, -89d, -91.0, -91.0 });
        Polygon outer = GeoTestUtil.createRegularPolygon(0, -90, atLeast(1000000), numVertices);

        document = new Document();
        addPolygonsToDoc(FIELDNAME, document, new Polygon(outer.getPolyLats(), outer.getPolyLons(), inner));
        writer.addDocument(document);

        ////// search /////
        // search an intersecting bbox
        IndexReader reader = DirectoryReader.open(writer);
        writer.close();
        IndexSearcher searcher = newSearcher(reader);
        Query q = newRectQuery(FIELDNAME, -1d, 1d, p.minLon, p.maxLon);
        assertEquals(1, searcher.count(q));

        // search a disjoint bbox
        q = newRectQuery(FIELDNAME, p.minLat - 1d, p.minLat + 1, p.minLon - 1d, p.minLon + 1d);
        assertEquals(0, searcher.count(q));

        // search a bbox in the hole
        q = newRectQuery(FIELDNAME, inner.minLat + 1e-6, inner.maxLat - 1e-6, inner.minLon + 1e-6, inner.maxLon - 1e-6);
        assertEquals(0, searcher.count(q));

        IOUtils.close(reader, dir);
    }

    public void testWithinDateLine() throws Exception {
        Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir);
        Document doc = new Document();

        Polygon indexPoly1 = new Polygon(new double[] { -7.5d, 15d, 15d, 0d, -7.5d }, new double[] { -180d, -180d, -176d, -176d, -180d });

        Polygon indexPoly2 = new Polygon(
            new double[] { 15d, -7.5d, -15d, -10d, 15d, 15d },
            new double[] { 180d, 180d, 176d, 174d, 176d, 180d }
        );

        Field[] fields = XLatLonShape.createIndexableFields("test", indexPoly1);
        for (Field f : fields) {
            doc.add(f);
        }
        fields = XLatLonShape.createIndexableFields("test", indexPoly2);
        for (Field f : fields) {
            doc.add(f);
        }
        w.addDocument(doc);
        w.forceMerge(1);

        ///// search //////
        IndexReader reader = w.getReader();
        w.close();
        IndexSearcher searcher = newSearcher(reader);

        Polygon[] searchPoly = new Polygon[] {
            new Polygon(new double[] { -20d, 20d, 20d, -20d, -20d }, new double[] { -180d, -180d, -170d, -170d, -180d }),
            new Polygon(new double[] { 20d, -20d, -20d, 20d, 20d }, new double[] { 180d, 180d, 170d, 170d, 180d }) };

        Query q = LatLonShape.newPolygonQuery("test", QueryRelation.WITHIN, searchPoly);
        assertEquals(1, searcher.count(q));

        q = LatLonShape.newPolygonQuery("test", QueryRelation.INTERSECTS, searchPoly);
        assertEquals(1, searcher.count(q));

        q = LatLonShape.newPolygonQuery("test", QueryRelation.DISJOINT, searchPoly);
        assertEquals(0, searcher.count(q));

        q = LatLonShape.newPolygonQuery("test", QueryRelation.CONTAINS, searchPoly);
        assertEquals(0, searcher.count(q));

        q = LatLonShape.newBoxQuery("test", QueryRelation.WITHIN, -20, 20, 170, -170);
        assertEquals(1, searcher.count(q));

        q = LatLonShape.newBoxQuery("test", QueryRelation.INTERSECTS, -20, 20, 170, -170);
        assertEquals(1, searcher.count(q));

        q = LatLonShape.newBoxQuery("test", QueryRelation.DISJOINT, -20, 20, 170, -170);
        assertEquals(0, searcher.count(q));

        q = LatLonShape.newBoxQuery("test", QueryRelation.CONTAINS, -20, 20, 170, -170);
        assertEquals(0, searcher.count(q));

        IOUtils.close(w, reader, dir);
    }

    public void testContainsDateLine() throws Exception {
        Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir);
        Document doc = new Document();

        Polygon indexPoly1 = new Polygon(new double[] { -2d, -2d, 2d, 2d, -2d }, new double[] { 178d, 180d, 180d, 178d, 178d });

        Polygon indexPoly2 = new Polygon(new double[] { -2d, -2d, 2d, 2d, -2d }, new double[] { -180d, -178d, -178d, -180d, -180d });

        Field[] fields = XLatLonShape.createIndexableFields("test", indexPoly1);
        for (Field f : fields) {
            doc.add(f);
        }
        fields = XLatLonShape.createIndexableFields("test", indexPoly2);
        for (Field f : fields) {
            doc.add(f);
        }
        w.addDocument(doc);
        w.forceMerge(1);

        ///// search //////
        IndexReader reader = w.getReader();
        w.close();
        IndexSearcher searcher = newSearcher(reader);

        Polygon[] searchPoly = new Polygon[] {
            new Polygon(new double[] { -1d, -1d, 1d, 1d, -1d }, new double[] { 179d, 180d, 180d, 179d, 179d }),
            new Polygon(new double[] { -1d, -1d, 1d, 1d, -1d }, new double[] { -180d, -179d, -179d, -180d, -180d }) };
        Query q;
        // Not supported due to encoding
        // Query q = LatLonShape.newPolygonQuery("test", QueryRelation.CONTAINS, searchPoly);
        // assertEquals(1, searcher.count(q));

        q = LatLonShape.newPolygonQuery("test", QueryRelation.INTERSECTS, searchPoly);
        assertEquals(1, searcher.count(q));

        q = LatLonShape.newPolygonQuery("test", QueryRelation.DISJOINT, searchPoly);
        assertEquals(0, searcher.count(q));

        q = LatLonShape.newPolygonQuery("test", QueryRelation.WITHIN, searchPoly);
        assertEquals(0, searcher.count(q));

        q = LatLonShape.newBoxQuery("test", QueryRelation.INTERSECTS, -1, 1, 179, -179);
        assertEquals(1, searcher.count(q));

        q = LatLonShape.newBoxQuery("test", QueryRelation.WITHIN, -1, 1, 179, -179);
        assertEquals(0, searcher.count(q));

        q = LatLonShape.newBoxQuery("test", QueryRelation.DISJOINT, -1, 1, 179, -179);
        assertEquals(0, searcher.count(q));

        IOUtils.close(w, reader, dir);
    }

    public void testLUCENE8454() throws Exception {
        Directory dir = newDirectory();
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

        Polygon poly = new Polygon(
            new double[] { -1.490648725633769E-132d, 90d, 90d, -1.490648725633769E-132d },
            new double[] { 0d, 0d, 180d, 0d }
        );

        Document document = new Document();
        addPolygonsToDoc(FIELDNAME, document, poly);
        writer.addDocument(document);

        ///// search //////
        IndexReader reader = writer.getReader();
        writer.close();
        IndexSearcher searcher = newSearcher(reader);

        // search a bbox in the hole
        Query q = LatLonShape.newBoxQuery(
            FIELDNAME,
            QueryRelation.DISJOINT,
            -29.46555603761226d,
            0.0d,
            8.381903171539307E-8d,
            0.9999999403953552d
        );
        assertEquals(1, searcher.count(q));

        IOUtils.close(reader, dir);
    }

    public void testPointIndexAndQuery() throws Exception {
        Directory dir = newDirectory();
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
        Document document = new Document();
        Point p = GeoTestUtil.nextPoint();
        double qLat = GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(p.getLat()));
        double qLon = GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(p.getLon()));
        p = new Point(qLat, qLon);
        Field[] fields = LatLonShape.createIndexableFields(FIELDNAME, p.getLat(), p.getLon());
        for (Field f : fields) {
            document.add(f);
        }
        writer.addDocument(document);

        //// search
        IndexReader r = writer.getReader();
        writer.close();
        IndexSearcher s = newSearcher(r);

        // search by same point
        Query q = LatLonShape.newPointQuery(FIELDNAME, QueryRelation.INTERSECTS, new double[] { p.getLat(), p.getLon() });
        assertEquals(1, s.count(q));
        IOUtils.close(r, dir);
    }

    public void testLUCENE8669() throws Exception {
        Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir);
        Document doc = new Document();

        Polygon indexPoly1 = new Polygon(new double[] { -7.5d, 15d, 15d, 0d, -7.5d }, new double[] { -180d, -180d, -176d, -176d, -180d });

        Polygon indexPoly2 = new Polygon(
            new double[] { 15d, -7.5d, -15d, -10d, 15d, 15d },
            new double[] { 180d, 180d, 176d, 174d, 176d, 180d }
        );

        addPolygonsToDoc(FIELDNAME, doc, indexPoly1);
        addPolygonsToDoc(FIELDNAME, doc, indexPoly2);
        w.addDocument(doc);
        w.forceMerge(1);

        ///// search //////
        IndexReader reader = w.getReader();
        w.close();
        IndexSearcher searcher = newSearcher(reader);

        Polygon[] searchPoly = new Polygon[] {
            new Polygon(new double[] { -20d, 20d, 20d, -20d, -20d }, new double[] { -180d, -180d, -170d, -170d, -180d }),
            new Polygon(new double[] { 20d, -20d, -20d, 20d, 20d }, new double[] { 180d, 180d, 170d, 170d, 180d }) };

        Query q = LatLonShape.newPolygonQuery(FIELDNAME, QueryRelation.WITHIN, searchPoly);
        assertEquals(1, searcher.count(q));

        q = LatLonShape.newPolygonQuery(FIELDNAME, QueryRelation.INTERSECTS, searchPoly);
        assertEquals(1, searcher.count(q));

        q = LatLonShape.newPolygonQuery(FIELDNAME, QueryRelation.DISJOINT, searchPoly);
        assertEquals(0, searcher.count(q));

        q = LatLonShape.newBoxQuery(FIELDNAME, QueryRelation.WITHIN, -20, 20, 170, -170);
        assertEquals(1, searcher.count(q));

        q = LatLonShape.newBoxQuery(FIELDNAME, QueryRelation.INTERSECTS, -20, 20, 170, -170);
        assertEquals(1, searcher.count(q));

        q = LatLonShape.newBoxQuery(FIELDNAME, QueryRelation.DISJOINT, -20, 20, 170, -170);
        assertEquals(0, searcher.count(q));

        IOUtils.close(w, reader, dir);
    }

    public void testLUCENE8679() {
        double alat = 1.401298464324817E-45;
        double alon = 24.76789767911785;
        double blat = 34.26468306870807;
        double blon = -52.67048754768767;
        Polygon polygon = new Polygon(
            new double[] { -14.448264200949083, 0, 0, -14.448264200949083, -14.448264200949083 },
            new double[] { 0.9999999403953552, 0.9999999403953552, 124.50086371762484, 124.50086371762484, 0.9999999403953552 }
        );
        Component2D polygon2D = LatLonGeometry.create(polygon);
        boolean intersects = polygon2D.intersectsTriangle(
            quantizeLon(alon),
            quantizeLat(blat),
            quantizeLon(blon),
            quantizeLat(blat),
            quantizeLon(alon),
            quantizeLat(alat)
        );

        assertTrue(intersects);

        intersects = polygon2D.intersectsTriangle(
            quantizeLon(alon),
            quantizeLat(blat),
            quantizeLon(alon),
            quantizeLat(alat),
            quantizeLon(blon),
            quantizeLat(blat)
        );

        assertTrue(intersects);
    }

    public void testTriangleTouchingEdges() {
        Polygon p = new Polygon(new double[] { 0, 0, 1, 1, 0 }, new double[] { 0, 1, 1, 0, 0 });
        Component2D polygon2D = LatLonGeometry.create(p);
        // 3 shared points
        boolean containsTriangle = polygon2D.containsTriangle(
            quantizeLon(0.5),
            quantizeLat(0),
            quantizeLon(1),
            quantizeLat(0.5),
            quantizeLon(0.5),
            quantizeLat(1)
        );
        boolean intersectsTriangle = polygon2D.intersectsTriangle(
            quantizeLon(0.5),
            quantizeLat(0),
            quantizeLon(1),
            quantizeLat(0.5),
            quantizeLon(0.5),
            quantizeLat(1)
        );
        assertTrue(intersectsTriangle);
        assertTrue(containsTriangle);
        // 2 shared points
        containsTriangle = polygon2D.containsTriangle(
            quantizeLon(0.5),
            quantizeLat(0),
            quantizeLon(1),
            quantizeLat(0.5),
            quantizeLon(0.5),
            quantizeLat(0.75)
        );
        intersectsTriangle = polygon2D.intersectsTriangle(
            quantizeLon(0.5),
            quantizeLat(0),
            quantizeLon(1),
            quantizeLat(0.5),
            quantizeLon(0.5),
            quantizeLat(0.75)
        );
        assertTrue(intersectsTriangle);
        assertTrue(containsTriangle);
        // 1 shared point
        containsTriangle = polygon2D.containsTriangle(
            quantizeLon(0.5),
            quantizeLat(0.5),
            quantizeLon(0.5),
            quantizeLat(0),
            quantizeLon(0.75),
            quantizeLat(0.75)
        );
        intersectsTriangle = polygon2D.intersectsTriangle(
            quantizeLon(0.5),
            quantizeLat(0),
            quantizeLon(1),
            quantizeLat(0.5),
            quantizeLon(0.5),
            quantizeLat(0.75)
        );
        assertTrue(intersectsTriangle);
        assertTrue(containsTriangle);
        // 1 shared point but out
        containsTriangle = polygon2D.containsTriangle(
            quantizeLon(1),
            quantizeLat(0.5),
            quantizeLon(2),
            quantizeLat(0),
            quantizeLon(2),
            quantizeLat(2)
        );
        intersectsTriangle = polygon2D.intersectsTriangle(
            quantizeLon(1),
            quantizeLat(0.5),
            quantizeLon(2),
            quantizeLat(0),
            quantizeLon(2),
            quantizeLat(2)
        );
        assertTrue(intersectsTriangle);
        assertFalse(containsTriangle);
        // 1 shared point but crossing
        containsTriangle = polygon2D.containsTriangle(
            quantizeLon(0.5),
            quantizeLat(0),
            quantizeLon(2),
            quantizeLat(0.5),
            quantizeLon(0.5),
            quantizeLat(1)
        );
        intersectsTriangle = polygon2D.intersectsTriangle(
            quantizeLon(0.5),
            quantizeLat(0),
            quantizeLon(2),
            quantizeLat(0.5),
            quantizeLon(0.5),
            quantizeLat(1)
        );
        assertTrue(intersectsTriangle);
        assertFalse(containsTriangle);
        // share one edge
        containsTriangle = polygon2D.containsTriangle(
            quantizeLon(0),
            quantizeLat(0),
            quantizeLon(0),
            quantizeLat(1),
            quantizeLon(0.5),
            quantizeLat(0.5)
        );
        intersectsTriangle = polygon2D.intersectsTriangle(
            quantizeLon(0),
            quantizeLat(0),
            quantizeLon(0),
            quantizeLat(1),
            quantizeLon(0.5),
            quantizeLat(0.5)
        );
        assertTrue(intersectsTriangle);
        assertTrue(containsTriangle);
        // share one edge outside
        containsTriangle = polygon2D.containsTriangle(
            quantizeLon(0),
            quantizeLat(1),
            quantizeLon(1.5),
            quantizeLat(1.5),
            quantizeLon(1),
            quantizeLat(1)
        );
        intersectsTriangle = polygon2D.intersectsTriangle(
            quantizeLon(0),
            quantizeLat(1),
            quantizeLon(1.5),
            quantizeLat(1.5),
            quantizeLon(1),
            quantizeLat(1)
        );
        assertTrue(intersectsTriangle);
        assertFalse(containsTriangle);
    }

    public void testLUCENE8736() throws Exception {
        Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir);

        // test polygons:
        Polygon indexPoly1 = new Polygon(new double[] { 4d, 4d, 3d, 3d, 4d }, new double[] { 3d, 4d, 4d, 3d, 3d });

        Polygon indexPoly2 = new Polygon(new double[] { 2d, 2d, 1d, 1d, 2d }, new double[] { 6d, 7d, 7d, 6d, 6d });

        Polygon indexPoly3 = new Polygon(new double[] { 1d, 1d, 0d, 0d, 1d }, new double[] { 3d, 4d, 4d, 3d, 3d });

        Polygon indexPoly4 = new Polygon(new double[] { 2d, 2d, 1d, 1d, 2d }, new double[] { 0d, 1d, 1d, 0d, 0d });

        // index polygons:
        Document doc;
        addPolygonsToDoc(FIELDNAME, doc = new Document(), indexPoly1);
        w.addDocument(doc);
        addPolygonsToDoc(FIELDNAME, doc = new Document(), indexPoly2);
        w.addDocument(doc);
        addPolygonsToDoc(FIELDNAME, doc = new Document(), indexPoly3);
        w.addDocument(doc);
        addPolygonsToDoc(FIELDNAME, doc = new Document(), indexPoly4);
        w.addDocument(doc);
        w.forceMerge(1);

        ///// search //////
        IndexReader reader = w.getReader();
        w.close();
        IndexSearcher searcher = newSearcher(reader);

        Polygon[] searchPoly = new Polygon[] { new Polygon(new double[] { 4d, 4d, 0d, 0d, 4d }, new double[] { 0d, 7d, 7d, 0d, 0d }) };

        Query q = LatLonShape.newPolygonQuery(FIELDNAME, QueryRelation.WITHIN, searchPoly);
        assertEquals(4, searcher.count(q));

        IOUtils.close(w, reader, dir);
    }

    public void testTriangleCrossingPolygonVertices() {
        Polygon p = new Polygon(new double[] { 0, 0, -5, -10, -5, 0 }, new double[] { -1, 1, 5, 0, -5, -1 });
        Component2D polygon2D = LatLonGeometry.create(p);
        boolean intersectsTriangle = polygon2D.intersectsTriangle(
            quantizeLon(-5),
            quantizeLat(0),
            quantizeLon(10),
            quantizeLat(0),
            quantizeLon(-5),
            quantizeLat(-15)
        );
        assertTrue(intersectsTriangle);
    }

    public void testLineCrossingPolygonVertices() {
        Polygon p = new Polygon(new double[] { 0, -1, 0, 1, 0 }, new double[] { -1, 0, 1, 0, -1 });
        Component2D polygon2D = LatLonGeometry.create(p);
        boolean intersectsTriangle = polygon2D.intersectsTriangle(
            quantizeLon(-1.5),
            quantizeLat(0),
            quantizeLon(1.5),
            quantizeLat(0),
            quantizeLon(-1.5),
            quantizeLat(0)
        );
        assertTrue(intersectsTriangle);
    }

    public void testLineSharedLine() {
        Line l = new Line(new double[] { 0, 0, 0, 0 }, new double[] { -2, -1, 0, 1 });
        Component2D l2d = LatLonGeometry.create(l);
        boolean intersectsLine = l2d.intersectsLine(quantizeLon(-5), quantizeLat(0), quantizeLon(5), quantizeLat(0));
        assertTrue(intersectsLine);
    }

    public void testLUCENE9055() throws Exception {
        Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir);

        // test polygons:
        // [5, 5], [10, 6], [10, 10], [5, 10], [5, 5] ]
        Polygon indexPoly1 = new Polygon(new double[] { 5d, 6d, 10d, 10d, 5d }, new double[] { 5d, 10d, 10d, 5d, 5d });

        // [ [6, 6], [9, 6], [9, 9], [6, 9], [6, 6] ]
        Polygon indexPoly2 = new Polygon(new double[] { 6d, 6d, 9d, 9d, 6d }, new double[] { 6d, 9d, 9d, 6d, 6d });

        // index polygons:
        Document doc;
        addPolygonsToDoc(FIELDNAME, doc = new Document(), indexPoly1);
        w.addDocument(doc);
        addPolygonsToDoc(FIELDNAME, doc = new Document(), indexPoly2);
        w.addDocument(doc);
        w.forceMerge(1);

        ///// search //////
        IndexReader reader = w.getReader();
        w.close();
        IndexSearcher searcher = newSearcher(reader);

        // [ [0, 0], [5, 5], [7, 7] ]
        Line searchLine = new Line(new double[] { 0, 5, 7 }, new double[] { 0, 5, 7 });

        Query q = LatLonShape.newLineQuery(FIELDNAME, QueryRelation.INTERSECTS, searchLine);
        assertEquals(2, searcher.count(q));

        IOUtils.close(w, reader, dir);
    }

    public void testIndexAndQuerySamePolygon() throws Exception {
        Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir);
        Document doc = new Document();
        Polygon polygon;
        while (true) {
            try {
                polygon = GeoTestUtil.nextPolygon();
                // quantize the polygon
                double[] lats = new double[polygon.numPoints()];
                double[] lons = new double[polygon.numPoints()];
                for (int i = 0; i < polygon.numPoints(); i++) {
                    lats[i] = GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(polygon.getPolyLat(i)));
                    lons[i] = GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(polygon.getPolyLon(i)));
                }
                polygon = new Polygon(lats, lons);
                XTessellator.tessellate(polygon);
                break;
            } catch (Exception e) {
                // invalid polygon, try a new one
            }
        }
        addPolygonsToDoc(FIELDNAME, doc, polygon);
        w.addDocument(doc);
        w.forceMerge(1);

        ///// search //////
        IndexReader reader = w.getReader();
        w.close();
        IndexSearcher searcher = newSearcher(reader);

        Query q = LatLonShape.newPolygonQuery(FIELDNAME, QueryRelation.WITHIN, polygon);
        assertEquals(1, searcher.count(q));
        q = LatLonShape.newPolygonQuery(FIELDNAME, QueryRelation.INTERSECTS, polygon);
        assertEquals(1, searcher.count(q));
        q = LatLonShape.newPolygonQuery(FIELDNAME, QueryRelation.DISJOINT, polygon);
        assertEquals(0, searcher.count(q));

        IOUtils.close(w, reader, dir);
    }

    public void testPointIndexAndDistanceQuery() throws Exception {
        Directory dir = newDirectory();
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
        Document document = new Document();
        Point p = GeoTestUtil.nextPoint();
        Field[] fields = LatLonShape.createIndexableFields(FIELDNAME, p.getLat(), p.getLon());
        for (Field f : fields) {
            document.add(f);
        }
        writer.addDocument(document);

        //// search
        IndexReader r = writer.getReader();
        writer.close();
        IndexSearcher s = newSearcher(r);

        double lat = GeoTestUtil.nextLatitude();
        double lon = GeoTestUtil.nextLongitude();
        final double radiusMeters = random().nextDouble() * GeoUtils.EARTH_MEAN_RADIUS_METERS * Math.PI / 2.0 + 1.0;
        Circle circle = new Circle(lat, lon, radiusMeters);
        Component2D circle2D = LatLonGeometry.create(circle);
        int expected;
        int expectedDisjoint;
        if (circle2D.contains(p.getLon(), p.getLat())) {
            expected = 1;
            expectedDisjoint = 0;
        } else {
            expected = 0;
            expectedDisjoint = 1;
        }

        Query q = LatLonShape.newDistanceQuery(FIELDNAME, QueryRelation.INTERSECTS, circle);
        assertEquals(expected, s.count(q));

        q = LatLonShape.newDistanceQuery(FIELDNAME, QueryRelation.WITHIN, circle);
        assertEquals(expected, s.count(q));

        q = LatLonShape.newDistanceQuery(FIELDNAME, QueryRelation.DISJOINT, circle);
        assertEquals(expectedDisjoint, s.count(q));

        IOUtils.close(r, dir);
    }

    public void testLucene9239() throws Exception {

        double[] lats = new double[] { -22.350172194105966, 90.0, 90.0, -22.350172194105966, -22.350172194105966 };
        double[] lons = new double[] { 49.931598911327825, 49.931598911327825, 51.40819689137876, 51.408196891378765, 49.931598911327825 };
        Polygon polygon = new Polygon(lats, lons);

        Directory dir = newDirectory();
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
        Document document = new Document();
        addPolygonsToDoc(FIELDNAME, document, polygon);
        writer.addDocument(document);

        //// search
        IndexReader r = writer.getReader();
        writer.close();
        IndexSearcher s = newSearcher(r);

        Circle circle = new Circle(78.01086555431775, 0.9513280497489234, 1097753.4254892308);
        // Circle is not within the polygon
        Query q = LatLonShape.newDistanceQuery(FIELDNAME, QueryRelation.CONTAINS, circle);
        assertEquals(0, s.count(q));

        IOUtils.close(r, dir);
    }

    public void testContainsWrappingBooleanQuery() throws Exception {

        double[] lats = new double[] { -30, -30, 30, 30, -30 };
        double[] lons = new double[] { -30, 30, 30, -30, -30 };
        Polygon polygon = new Polygon(lats, lons);

        Directory dir = newDirectory();
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
        Document document = new Document();
        addPolygonsToDoc(FIELDNAME, document, polygon);
        writer.addDocument(document);

        //// search
        IndexReader r = writer.getReader();
        writer.close();
        IndexSearcher s = newSearcher(r);

        LatLonGeometry[] geometries = new LatLonGeometry[] { new Rectangle(0, 1, 0, 1), new Point(4, 4) };
        // geometries within the polygon
        Query q = LatLonShape.newGeometryQuery(FIELDNAME, QueryRelation.CONTAINS, geometries);
        TopDocs topDocs = s.search(q, 1);
        assertEquals(1, topDocs.scoreDocs.length);
        assertEquals(1.0, topDocs.scoreDocs[0].score, 0.0);
        IOUtils.close(r, dir);
    }

    public void testContainsIndexedGeometryCollection() throws Exception {
        Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir);
        Polygon polygon = new Polygon(new double[] { -64, -64, 64, 64, -64 }, new double[] { -132, 132, 132, -132, -132 });
        Field[] polygonFields = XLatLonShape.createIndexableFields(FIELDNAME, polygon);
        // POINT(5, 5) inside the indexed polygon
        Field[] pointFields = LatLonShape.createIndexableFields(FIELDNAME, 5, 5);
        int numDocs = random().nextInt(1000);
        // index the same multi geometry many times
        for (int i = 0; i < numDocs; i++) {
            Document doc = new Document();
            for (Field f : polygonFields) {
                doc.add(f);
            }
            for (int j = 0; j < 10; j++) {
                for (Field f : pointFields) {
                    doc.add(f);
                }
            }
            w.addDocument(doc);
        }
        w.forceMerge(1);

        ///// search //////
        IndexReader reader = w.getReader();
        w.close();
        IndexSearcher searcher = newSearcher(reader);
        // Contains is only true if the query geometry is inside a geometry and does not intersect with any other geometry
        // belonging to the same document. In this case the query geometry contains the indexed polygon but the point is
        // inside the query as well, hence the result is 0.
        Polygon polygonQuery = new Polygon(new double[] { 4, 4, 6, 6, 4 }, new double[] { 4, 6, 6, 4, 4 });
        Query query = LatLonShape.newGeometryQuery(FIELDNAME, QueryRelation.CONTAINS, polygonQuery);
        assertEquals(0, searcher.count(query));

        Rectangle rectangle = new Rectangle(4.0, 6.0, 4.0, 6.0);
        query = LatLonShape.newGeometryQuery(FIELDNAME, QueryRelation.CONTAINS, rectangle);
        assertEquals(0, searcher.count(query));

        Circle circle = new Circle(5, 5, 10000);
        query = LatLonShape.newGeometryQuery(FIELDNAME, QueryRelation.CONTAINS, circle);
        assertEquals(0, searcher.count(query));

        IOUtils.close(w, reader, dir);
    }
}
