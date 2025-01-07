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
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.Orientation;
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
import org.elasticsearch.test.hamcrest.WellKnownBinaryBytesRefMatcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;

public class AbstractShapeGeometryFieldMapperTests extends ESTestCase {
    public void testCartesianBoundsBlockLoader() throws IOException {
        testBoundsBlockLoaderAux(
            CoordinateEncoder.CARTESIAN,
            () -> ShapeTestUtils.randomGeometryWithoutCircle(0, false),
            CartesianShapeIndexer::new,
            SpatialEnvelopeVisitor::visitCartesian
        );
    }

    // TODO when we turn this optimization on for geo, this test should pass.
    public void ignoreTestGeoBoundsBlockLoader() throws IOException {
        testBoundsBlockLoaderAux(
            CoordinateEncoder.GEO,
            () -> GeometryTestUtils.randomGeometryWithoutCircle(0, false),
            field -> new GeoShapeIndexer(Orientation.RIGHT, field),
            g -> SpatialEnvelopeVisitor.visitGeo(g, SpatialEnvelopeVisitor.WrapLongitude.WRAP)
        );
    }

    private static void testBoundsBlockLoaderAux(
        CoordinateEncoder encoder,
        Supplier<Geometry> generator,
        Function<String, ShapeIndexer> indexerFactory,
        Function<Geometry, Optional<Rectangle>> visitor
    ) throws IOException {
        var geometries = IntStream.range(0, 50).mapToObj(i -> generator.get()).toList();
        var loader = new AbstractShapeGeometryFieldMapper.AbstractShapeGeometryFieldType.BoundsBlockLoader("field", encoder);
        try (Directory directory = newDirectory()) {
            try (var iw = new RandomIndexWriter(random(), directory)) {
                for (Geometry geometry : geometries) {
                    var shape = new BinaryShapeDocValuesField("field", encoder);
                    shape.add(indexerFactory.apply("field").indexShape(geometry), geometry);
                    var doc = new Document();
                    doc.add(shape);
                    iw.addDocument(doc);
                }
            }

            var expected = new ArrayList<Rectangle>();
            var byteRefResults = new ArrayList<BytesRef>();
            int currentIndex = 0;
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                for (var leaf : reader.leaves()) {
                    LeafReader leafReader = leaf.reader();
                    int numDocs = leafReader.numDocs();
                    // We specifically check just the even indices, to verify the loader can skip documents correctly.
                    int[] array = evenArray(numDocs);
                    for (int i = 0; i < array.length; i += 1) {
                        expected.add(visitor.apply(geometries.get(array[i] + currentIndex)).get());
                    }
                    try (var block = (TestBlock) loader.reader(leaf).read(TestBlock.factory(leafReader.numDocs()), TestBlock.docs(array))) {
                        for (int i = 0; i < block.size(); i++) {
                            byteRefResults.add((BytesRef) block.get(i));
                        }
                    }
                    currentIndex += numDocs;
                }
            }

            for (int i = 0; i < expected.size(); i++) {
                Rectangle rectangle = expected.get(i);
                var geoString = rectangle.toString();
                assertThat(
                    Strings.format("geometry '%s' wasn't extracted correctly", geoString),
                    byteRefResults.get(i),
                    WellKnownBinaryBytesRefMatcher.encodes(RectangleMatcher.closeToFloat(rectangle, 1e-3, encoder))
                );
            }
        }
    }

    private static int[] evenArray(int maxIndex) {
        return IntStream.range(0, maxIndex / 2).map(x -> x * 2).toArray();
    }
}
