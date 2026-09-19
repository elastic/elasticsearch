/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues.fn;

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
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.index.mapper.TestBlock;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.index.mapper.blockloader.MockWarnings;
import org.elasticsearch.lucene.spatial.CentroidCalculator;
import org.elasticsearch.lucene.spatial.CoordinateEncoder;
import org.elasticsearch.lucene.spatial.GeometryDocValueReader;
import org.elasticsearch.lucene.spatial.GeometryDocValueWriter;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Checks that {@link GeoGridFromShapeDocValuesBlockLoader} hands every document's binary doc value to the tiler and
 * turns the returned cells into null, single or multi-valued positions. The tiler itself is supplied by ES|QL and
 * tested there; here a stand-in derives "cells" from the extent of the indexed shape so the expected values can be
 * recomputed independently from the doc values.
 */
public class GeoGridFromShapeDocValuesBlockLoaderTests extends ESTestCase {
    private static final String FIELD = "field";
    private static final GeoShapeIndexer INDEXER = new GeoShapeIndexer(Orientation.CCW, FIELD);

    /** Points get one "cell", rectangles two, unless the extent is entirely west of the meridian, which gets none. */
    static List<Long> extentCells(BytesRef encodedShape) throws IOException {
        GeometryDocValueReader reader = new GeometryDocValueReader();
        reader.reset(encodedShape);
        var extent = reader.getExtent();
        if (extent.maxX() < 0) {
            return List.of();
        }
        if (extent.minX() == extent.maxX() && extent.minY() == extent.maxY()) {
            return List.of((long) extent.minX());
        }
        return List.of((long) extent.minX(), (long) extent.maxX());
    }

    public void testLoadsCellsPerDocument() throws IOException {
        List<Geometry> geometries = new ArrayList<>();
        int docCount = between(50, 500);
        for (int i = 0; i < docCount; i++) {
            geometries.add(switch (between(0, 3)) {
                case 0 -> null; // no value
                case 1 -> new Point(randomDoubleBetween(-180, 180, true), randomDoubleBetween(-80, 80, true));
                case 2 -> new Rectangle(-170, -100, 40, -40); // no cells from the stand-in tiler
                default -> new Rectangle(-10, 60, 50, -20);
            });
        }
        MockWarnings warnings = new MockWarnings();
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(5));
        try (Directory dir = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {
            for (Geometry geometry : geometries) {
                List<IndexableField> doc = new ArrayList<>(1);
                if (geometry != null) {
                    CentroidCalculator centroid = new CentroidCalculator();
                    centroid.add(geometry);
                    doc.add(
                        new BinaryDocValuesField(
                            FIELD,
                            GeometryDocValueWriter.write(INDEXER.indexShape(geometry), CoordinateEncoder.GEO, centroid)
                        )
                    );
                }
                iw.addDocument(doc);
            }
            iw.forceMerge(1);
            try (DirectoryReader dr = iw.getReader()) {
                LeafReaderContext ctx = getOnlyLeafReader(dr).getContext();
                List<Object> expected = expectedCells(ctx);

                var config = new BlockLoaderFunctionConfig.GeoGrid(
                    BlockLoaderFunctionConfig.Function.ST_GEOHASH,
                    3,
                    null,
                    () -> (lon, lat) -> {
                        throw new AssertionError("points are not loaded by the shape loader");
                    },
                    w -> {
                        assertThat("the loader must pass its warnings to the tiler", w, sameInstance(warnings));
                        return GeoGridFromShapeDocValuesBlockLoaderTests::extentCells;
                    }
                );
                var loader = new GeoGridFromShapeDocValuesBlockLoader(FIELD, config, warnings);
                assertThat(loader, hasToString("GeoGridFromShapeDocValues[field, ST_GEOHASH, 3]"));
                try (BlockLoader.ColumnAtATimeReader reader = loader.reader(breaker, ctx)) {
                    assertThat(reader, hasToString("GeoGridFromShapeDocValues"));
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

    public void testMissingFieldLoadsNulls() throws IOException {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(5));
        try (Directory dir = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {
            iw.addDocument(List.of());
            iw.addDocument(List.of());
            try (DirectoryReader dr = iw.getReader()) {
                LeafReaderContext ctx = getOnlyLeafReader(dr).getContext();
                var config = new BlockLoaderFunctionConfig.GeoGrid(
                    BlockLoaderFunctionConfig.Function.ST_GEOTILE,
                    5,
                    null,
                    () -> (lon, lat) -> 0,
                    w -> encoded -> {
                        throw new AssertionError("no doc values to tile");
                    }
                );
                var loader = new GeoGridFromShapeDocValuesBlockLoader(FIELD, config, null);
                try (BlockLoader.ColumnAtATimeReader reader = loader.reader(breaker, ctx)) {
                    try (TestBlock block = (TestBlock) reader.read(TestBlock.factory(), TestBlock.docs(ctx), 0, false)) {
                        assertThat(block.size(), equalTo(2));
                        assertThat(block.get(0), nullValue());
                        assertThat(block.get(1), nullValue());
                    }
                }
                assertThat(breaker.getUsed(), equalTo(0L));
            }
        }
    }

    /** Recomputes the expected value of every document straight from the doc values, in index order. */
    private static List<Object> expectedCells(LeafReaderContext ctx) throws IOException {
        var docValues = ctx.reader().getBinaryDocValues(FIELD);
        List<Object> expected = new ArrayList<>();
        for (int doc = 0; doc < ctx.reader().numDocs(); doc++) {
            if (docValues == null || docValues.advanceExact(doc) == false) {
                expected.add(null);
                continue;
            }
            List<Long> cells = extentCells(docValues.binaryValue());
            expected.add(cells.isEmpty() ? null : cells.size() == 1 ? cells.get(0) : cells);
        }
        return expected;
    }
}
