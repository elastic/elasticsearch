/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.utils.WellKnownBinary;
import org.elasticsearch.index.mapper.AbstractShapeGeometryFieldMapper.AbstractShapeGeometryFieldType.GeometryBlockLoader;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.index.mapper.blockloader.MockWarnings;
import org.elasticsearch.lucene.spatial.CentroidCalculator;
import org.elasticsearch.lucene.spatial.CoordinateEncoder;
import org.elasticsearch.lucene.spatial.GeometryDocValueReader;
import org.elasticsearch.lucene.spatial.GeometryDocValueWriter;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Checks that {@link GeometryBlockLoader} loads the geometry reconstructed from every document's doc values as WKB, or,
 * when configured with a {@link BlockLoaderFunctionConfig.GeometryTransform}, hands it to the transformer instead. The
 * real transformers are supplied by ES|QL and tested there; here a stand-in returns null for points and the WKB of
 * anything else, so the expected values can be recomputed independently from the doc values.
 */
public class ShapeGeometryBlockLoaderTests extends ESTestCase {
    private static final String FIELD = "field";
    private static final GeoShapeIndexer INDEXER = new GeoShapeIndexer(Orientation.CCW, FIELD);

    public void testLoadsWkb() throws IOException {
        assertLoads(new GeometryBlockLoader(FIELD, CoordinateEncoder.GEO), "GeometryFromShapeDocValues[field]", false);
    }

    public void testLoadsTransformed() throws IOException {
        MockWarnings warnings = new MockWarnings();
        var transform = new BlockLoaderFunctionConfig.GeometryTransform(BlockLoaderFunctionConfig.Function.ST_SIMPLIFY, 1.5, w -> {
            assertThat("the loader must pass its warnings to the transformer", w, sameInstance(warnings));
            return geometry -> geometry instanceof Point ? null : toWkb(geometry);
        });
        assertLoads(
            new GeometryBlockLoader(FIELD, CoordinateEncoder.GEO, transform, warnings),
            "GeometryTransformFromShapeDocValues[field, ST_SIMPLIFY]",
            true
        );
    }

    public void testTransformEqualityIgnoresTransformers() {
        var transform = new BlockLoaderFunctionConfig.GeometryTransform(
            BlockLoaderFunctionConfig.Function.ST_SIMPLIFY,
            1.5,
            w -> g -> null
        );
        var same = new BlockLoaderFunctionConfig.GeometryTransform(BlockLoaderFunctionConfig.Function.ST_SIMPLIFY, 1.5, w -> g -> null);
        var other = new BlockLoaderFunctionConfig.GeometryTransform(BlockLoaderFunctionConfig.Function.ST_SIMPLIFY, 2, w -> g -> null);
        assertEquals(transform, same);
        assertEquals(transform.hashCode(), same.hashCode());
        assertNotEquals(transform, other);
        expectThrows(
            IllegalArgumentException.class,
            () -> new BlockLoaderFunctionConfig.GeometryTransform(BlockLoaderFunctionConfig.Function.ST_GEOHASH, 1, w -> g -> null)
        );
    }

    private void assertLoads(GeometryBlockLoader loader, String loaderDescription, boolean pointsAreNull) throws IOException {
        assertThat(loader, hasToString(loaderDescription));
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(5));
        try (Directory dir = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {
            int docCount = between(50, 300);
            for (int i = 0; i < docCount; i++) {
                List<IndexableField> doc = new ArrayList<>(1);
                Geometry geometry = randomGeometry();
                if (geometry != null) {
                    CentroidCalculator centroid = new CentroidCalculator();
                    centroid.add(geometry);
                    doc.add(
                        new BinaryDocValuesField(
                            FIELD,
                            GeometryDocValueWriter.write(INDEXER.indexShape(geometry), CoordinateEncoder.GEO, centroid, List.of(geometry))
                        )
                    );
                }
                iw.addDocument(doc);
            }
            iw.forceMerge(1);
            try (DirectoryReader dr = iw.getReader()) {
                LeafReaderContext ctx = getOnlyLeafReader(dr).getContext();
                List<BytesRef> expected = expected(ctx, pointsAreNull);
                try (BlockLoader.ColumnAtATimeReader reader = loader.reader(breaker, ctx)) {
                    assertThat(reader, hasToString("GeometryFromShapeDocValues"));
                    try (TestBlock block = (TestBlock) reader.read(TestBlock.factory(), TestBlock.docs(ctx), 0, false)) {
                        assertThat(block.size(), equalTo(ctx.reader().numDocs()));
                        for (int i = 0; i < block.size(); i++) {
                            assertThat("doc " + i, block.get(i), equalTo(expected.get(i)));
                        }
                    }
                }
                assertThat("nothing left on the breaker after close", breaker.getUsed(), equalTo(0L));
            }
        }
    }

    private static Geometry randomGeometry() {
        double x = randomDoubleBetween(-170, 160, true);
        double y = randomDoubleBetween(-80, 70, true);
        return switch (between(0, 3)) {
            case 0 -> null; // no value
            case 1 -> new Point(x, y);
            case 2 -> new Line(new double[] { x, x + 3, x + 5 }, new double[] { y, y + 4, y + 1 });
            default -> new Polygon(new LinearRing(new double[] { x, x + 5, x + 5, x, x }, new double[] { y, y, y + 5, y + 5, y }));
        };
    }

    /** Recomputes the expected value of every document straight from the doc values, in index order. */
    private static List<BytesRef> expected(LeafReaderContext ctx, boolean pointsAreNull) throws IOException {
        var docValues = ctx.reader().getBinaryDocValues(FIELD);
        GeometryDocValueReader reader = new GeometryDocValueReader();
        List<BytesRef> expected = new ArrayList<>();
        for (int doc = 0; doc < ctx.reader().numDocs(); doc++) {
            if (docValues == null || docValues.advanceExact(doc) == false) {
                expected.add(null);
                continue;
            }
            reader.reset(docValues.binaryValue());
            Geometry geometry = reader.getGeometry(CoordinateEncoder.GEO);
            expected.add(pointsAreNull && geometry instanceof Point ? null : toWkb(geometry));
        }
        return expected;
    }

    private static BytesRef toWkb(Geometry geometry) {
        return new BytesRef(WellKnownBinary.toWKB(geometry, ByteOrder.LITTLE_ENDIAN));
    }
}
