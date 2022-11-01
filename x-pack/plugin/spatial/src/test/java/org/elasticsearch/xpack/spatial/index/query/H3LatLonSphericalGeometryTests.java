/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.query;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.document.ShapeField;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.h3.CellBoundary;
import org.elasticsearch.h3.H3;
import org.elasticsearch.h3.LatLng;

public class H3LatLonSphericalGeometryTests extends H3LatLonGeometryTests {

    @Override
    protected H3LatLonGeometry makeGeometry(String h3Address) {
        return new H3LatLonGeometry.Spherical(h3Address);
    }

    @Override
    protected H3LatLonGeometry makeGeometry(String h3Address, double scaleFactor) {
        return new H3LatLonGeometry.Spherical.Scaled(h3Address, scaleFactor);
    }

    @Override
    protected double getLatitudeThreshold() {
        return 90d;
    }

    /**
     * This test is specific to the `geo_point` field type, as indicated by the use of LatLonPoint.
     * As such it only works with the H3LatLonGeometry.Spherical class, since that was originally developed for this field type.
     */
    public void testIndexPoints() throws Exception {
        Point queryPoint = GeometryTestUtils.randomPoint();
        String[] hexes = new String[H3.MAX_H3_RES + 1];
        for (int res = 0; res < hexes.length; res++) {
            hexes[res] = H3.geoToH3Address(queryPoint.getLat(), queryPoint.getLon(), res);
        }
        IndexWriterConfig iwc = newIndexWriterConfig();
        // Else seeds may not reproduce:
        iwc.setMergeScheduler(new SerialMergeScheduler());
        // Else we can get O(N^2) merging:
        iwc.setMaxBufferedDocs(10);
        Directory dir = newDirectory();
        // RandomIndexWriter is too slow here:
        int[] counts = new int[H3.MAX_H3_RES + 1];
        IndexWriter w = new IndexWriter(dir, iwc);
        for (String hex : hexes) {
            CellBoundary cellBoundary = H3.h3ToGeoBoundary(hex);
            for (int i = 0; i < cellBoundary.numPoints(); i++) {
                Document doc = new Document();
                LatLng latLng = cellBoundary.getLatLon(i);
                doc.add(new LatLonPoint(FIELD_NAME, latLng.getLatDeg(), latLng.getLonDeg()));
                w.addDocument(doc);
                computeCounts(hexes, latLng.getLonDeg(), latLng.getLatDeg(), counts);
            }

        }
        final int numDocs = randomIntBetween(1000, 2000);
        for (int id = 0; id < numDocs; id++) {
            Document doc = new Document();
            Point point = GeometryTestUtils.randomPoint();
            doc.add(new LatLonPoint(FIELD_NAME, point.getLat(), point.getLon()));
            w.addDocument(doc);
            computeCounts(hexes, point.getLon(), point.getLat(), counts);
        }

        if (random().nextBoolean()) {
            w.forceMerge(1);
        }
        final IndexReader r = DirectoryReader.open(w);
        w.close();

        IndexSearcher s = newSearcher(r);
        for (int i = 0; i < H3.MAX_H3_RES + 1; i++) {
            H3LatLonGeometry geometry = makeGeometry(hexes[i]);
            Query indexQuery = LatLonPoint.newGeometryQuery(FIELD_NAME, ShapeField.QueryRelation.INTERSECTS, geometry);
            assertEquals(counts[i], s.count(indexQuery));
        }
        IOUtils.close(r, dir);
    }

    private void computeCounts(String[] hexes, double lon, double lat, int[] counts) {
        double qLat = GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(lat));
        double qLon = GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(lon));
        for (int res = 0; res < hexes.length; res++) {
            if (hexes[res].equals(H3.geoToH3Address(qLat, qLon, res))) {
                counts[res]++;
            }
        }
    }
}
