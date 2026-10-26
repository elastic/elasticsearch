/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.common.spatial;

import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.lucene.spatial.CentroidCalculator;
import org.elasticsearch.lucene.spatial.CoordinateEncoder;
import org.elasticsearch.lucene.spatial.GeometryDocValueWriter;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * {@link GeoShapeDocValues} must read both doc-value formats written by the server: the legacy triangle tree with
 * inline coordinates, and the V2 tree that references a vertex table. The same geometry written in each format must
 * produce identical headers, intersections and grid cells.
 */
public class GeoShapeDocValuesTests extends ESTestCase {
    private static final GeoShapeIndexer INDEXER = new GeoShapeIndexer(Orientation.CCW, "test");

    public void testLegacyAndV2FormatsAgree() throws IOException {
        Geometry geometry = randomValueOtherThanMany(g -> INDEXER.indexShape(g).isEmpty(), GeoShapeDocValuesTests::randomShape);
        List<IndexableField> fields = INDEXER.indexShape(geometry);
        CentroidCalculator centroid = new CentroidCalculator();
        centroid.add(geometry);
        BytesRef legacyBytes = GeometryDocValueWriter.writeLegacy(fields, CoordinateEncoder.GEO, centroid);
        BytesRef v2Bytes = GeometryDocValueWriter.writeV2(fields, CoordinateEncoder.GEO, centroid, List.of(geometry));
        assertThat(v2Bytes, not(equalTo(legacyBytes)));

        GeoShapeDocValues legacy = GeoShapeDocValues.fromDocValue(legacyBytes);
        GeoShapeDocValues v2 = GeoShapeDocValues.fromDocValue(v2Bytes);
        assertThat(v2.minLon(), equalTo(legacy.minLon()));
        assertThat(v2.maxLon(), equalTo(legacy.maxLon()));
        assertThat(v2.minLat(), equalTo(legacy.minLat()));
        assertThat(v2.maxLat(), equalTo(legacy.maxLat()));
        assertThat(v2.centroidLon(), equalTo(legacy.centroidLon()));
        assertThat(v2.centroidLat(), equalTo(legacy.centroidLat()));
        assertThat(v2.isSinglePoint(), equalTo(legacy.isSinglePoint()));

        for (int i = 0; i < 10; i++) {
            Rectangle r = GeometryTestUtils.randomRectangle();
            Component2D component = LatLonGeometry.create(
                new org.apache.lucene.geo.Rectangle(r.getMinLat(), r.getMaxLat(), r.getMinLon(), r.getMaxLon())
            );
            assertThat(v2.intersects(component), equalTo(legacy.intersects(component)));
        }

        for (int precision = 0; precision <= 2; precision++) {
            GeoHexGridTiler tiler = GeoHexGridTiler.makeGridTiler(precision, null);
            long[] legacyCells = tiler.cells(legacy, Integer.MAX_VALUE, m -> {});
            long[] v2Cells = tiler.cells(v2, Integer.MAX_VALUE, m -> {});
            assertThat("precision " + precision, v2Cells, equalTo(legacyCells));
        }
    }

    private static Geometry randomShape() {
        return switch (between(0, 5)) {
            case 0 -> GeometryTestUtils.randomLine(false);
            case 1 -> GeometryTestUtils.randomMultiLine(false);
            case 2 -> GeometryTestUtils.randomPolygon(false);
            case 3 -> GeometryTestUtils.randomMultiPolygon(false);
            case 4 -> GeometryTestUtils.randomRectangle();
            default -> GeometryTestUtils.randomMultiPoint(false);
        };
    }
}
