/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Document;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.geo.GeometryNormalizer;
import org.elasticsearch.core.Strings;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geo.ShapeTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.SpatialEnvelopeVisitor;
import org.elasticsearch.lucene.spatial.BinaryShapeDocValuesField;
import org.elasticsearch.lucene.spatial.CartesianShapeIndexer;
import org.elasticsearch.lucene.spatial.CoordinateEncoder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.hamcrest.RectangleMatcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.apache.lucene.geo.GeoEncodingUtils.decodeLongitude;
import static org.elasticsearch.common.geo.Orientation.RIGHT;

public class AbstractShapeGeometryFieldMapperTests extends ESTestCase {
    public void testCartesianBoundsBlockLoader() throws IOException {
        testBoundsBlockLoader(
            CoordinateEncoder.CARTESIAN,
            () -> ShapeTestUtils.randomGeometryWithoutCircle(0, false),
            CartesianShapeIndexer::new,
            SpatialEnvelopeVisitor::visitCartesian,
            AbstractShapeGeometryFieldMapperTests::makeCartesianRectangle
        );
    }

    // TODO: Re-enable this test after fixing the bug in the ShapeEnvelopeVisitor regarding Rectangle crossing the dateline
    // Currently it is flaky if the geometries include a Rectangle like one defined in the test below
    public void ignoreTestGeoBoundsBlockLoader() throws IOException {
        testBoundsBlockLoader(
            CoordinateEncoder.GEO,
            () -> normalize(GeometryTestUtils.randomGeometryWithoutCircle(0, false)),
            field -> new GeoShapeIndexer(RIGHT, field),
            g -> SpatialEnvelopeVisitor.visitGeo(g, SpatialEnvelopeVisitor.WrapLongitude.WRAP),
            AbstractShapeGeometryFieldMapperTests::makeGeoRectangle
        );
    }

    // TODO: Re-enable this test after fixing the bug in the SpatialEnvelopeVisitor regarding Rectangle crossing the dateline
    // See the difference between GeoShapeIndexer.visitRectangle() and SpatialEnvelopeVisitor.GeoPointVisitor.visitRectangle()
    public void ignoreTestRectangleCrossingDateline() throws IOException {
        var geometries = new ArrayList<Geometry>();
        geometries.add(new Rectangle(180, 51.62247094594227, -18.5, -24.902304006345503));
        testBoundsBlockLoaderAux(
            CoordinateEncoder.GEO,
            geometries,
            field -> new GeoShapeIndexer(RIGHT, field),
            g -> SpatialEnvelopeVisitor.visitGeo(g, SpatialEnvelopeVisitor.WrapLongitude.WRAP),
            AbstractShapeGeometryFieldMapperTests::makeGeoRectangle
        );
    }

    private Geometry normalize(Geometry geometry) {
        return GeometryNormalizer.needsNormalize(RIGHT, geometry) ? GeometryNormalizer.apply(RIGHT, geometry) : geometry;
    }

    private static void testBoundsBlockLoader(
        CoordinateEncoder encoder,
        Supplier<Geometry> generator,
        Function<String, ShapeIndexer> indexerFactory,
        Function<Geometry, Optional<Rectangle>> visitor,
        BiFunction<CoordinateEncoder, Object, Rectangle> rectangleMaker
    ) throws IOException {
        var geometries = IntStream.range(0, 50).mapToObj(i -> generator.get()).toList();
        testBoundsBlockLoaderAux(encoder, geometries, indexerFactory, visitor, rectangleMaker);
    }

    private static void testBoundsBlockLoaderAux(
        CoordinateEncoder encoder,
        java.util.List<Geometry> geometries,
        Function<String, ShapeIndexer> indexerFactory,
        Function<Geometry, Optional<Rectangle>> visitor,
        BiFunction<CoordinateEncoder, Object, Rectangle> rectangleMaker
    ) throws IOException {
        var loader = new AbstractShapeGeometryFieldMapper.AbstractShapeGeometryFieldType.BoundsBlockLoader("field");
        try (Directory directory = newDirectory()) {
            // Since we also test that the documents are loaded in the correct order, we need to write them in order, so we can't use
            // RandomIndexWriter here.
            try (var iw = new IndexWriter(directory, new IndexWriterConfig(null /* analyzer */))) {
                for (Geometry geometry : geometries) {
                    var shape = new BinaryShapeDocValuesField("field", encoder);
                    shape.add(indexerFactory.apply("field").indexShape(geometry), geometry);
                    var doc = new Document();
                    doc.add(shape);
                    iw.addDocument(doc);
                }
            }

            var expected = new ArrayList<Rectangle>();
            ArrayList<Object> intArrayResults = new ArrayList<>();
            int currentIndex = 0;
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                for (var leaf : reader.leaves()) {
                    LeafReader leafReader = leaf.reader();
                    int numDocs = leafReader.numDocs();
                    // We specifically check just the even indices, to verify the loader can skip documents correctly.
                    int[] array = evenArray(numDocs);
                    for (int j : array) {
                        expected.add(visitor.apply(geometries.get(j + currentIndex)).get());
                    }
                    try (var block = (TestBlock) loader.reader(leaf).read(TestBlock.factory(leafReader.numDocs()), TestBlock.docs(array))) {
                        for (int i = 0; i < block.size(); i++) {
                            intArrayResults.add(block.get(i));
                        }
                    }
                    currentIndex += numDocs;
                }
            }

            for (int i = 0; i < expected.size(); i++) {
                Rectangle rectangle = expected.get(i);
                var geoString = rectangle.toString();
                Rectangle result = rectangleMaker.apply(encoder, intArrayResults.get(i));
                assertThat(
                    Strings.format("geometry[%d] '%s' wasn't extracted correctly", i, geoString),
                    result,
                    RectangleMatcher.closeToFloat(rectangle, 1e-3, encoder)
                );
            }
        }
    }

    private static Rectangle makeCartesianRectangle(CoordinateEncoder encoder, Object integers) {
        if (integers instanceof ArrayList<?> list) {
            int[] ints = list.stream().mapToInt(x -> (int) x).toArray();
            if (list.size() == 6) {
                // Data in order defined by Extent class
                double top = encoder.decodeY(ints[0]);
                double bottom = encoder.decodeY(ints[1]);
                double negLeft = encoder.decodeX(ints[2]);
                double negRight = encoder.decodeX(ints[3]);
                double posLeft = encoder.decodeX(ints[4]);
                double posRight = encoder.decodeX(ints[5]);
                return new Rectangle(Math.min(negLeft, posLeft), Math.max(negRight, posRight), top, bottom);
            } else if (list.size() == 4) {
                // Data in order defined by Rectangle class
                return new Rectangle(
                    encoder.decodeX(ints[0]),
                    encoder.decodeX(ints[1]),
                    encoder.decodeY(ints[2]),
                    encoder.decodeY(ints[3])
                );
            } else {
                throw new IllegalArgumentException("Expected 4 or 6 integers");
            }
        }
        throw new IllegalArgumentException("Expected an array of integers");
    }

    private static Rectangle makeGeoRectangle(CoordinateEncoder encoder, Object integers) {
        if (integers instanceof ArrayList<?> list) {
            int[] ints = list.stream().mapToInt(x -> (int) x).toArray();
            if (list.size() != 6) {
                throw new IllegalArgumentException("Expected 6 integers");
            }
            // Data in order defined by Extent class
            return asGeoRectangle(ints[0], ints[1], ints[2], ints[3], ints[4], ints[5]);
        }
        throw new IllegalArgumentException("Expected an array of integers");
    }

    private static Rectangle asGeoRectangle(int top, int bottom, int negLeft, int negRight, int posLeft, int posRight) {
        return SpatialEnvelopeVisitor.GeoPointVisitor.getResult(
            GeoEncodingUtils.decodeLatitude(top),
            GeoEncodingUtils.decodeLatitude(bottom),
            negLeft <= 0 ? decodeLongitude(negLeft) : Double.POSITIVE_INFINITY,
            negRight <= 0 ? decodeLongitude(negRight) : Double.NEGATIVE_INFINITY,
            posLeft >= 0 ? decodeLongitude(posLeft) : Double.POSITIVE_INFINITY,
            posRight >= 0 ? decodeLongitude(posRight) : Double.NEGATIVE_INFINITY,
            SpatialEnvelopeVisitor.WrapLongitude.WRAP
        );
    }

    private static int[] evenArray(int maxIndex) {
        return IntStream.range(0, maxIndex / 2).map(x -> x * 2).toArray();
    }
}
