/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues.fn;

import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.TestBlock;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.index.mapper.blockloader.docvalues.LongsBlockLoader;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashBoundedPredicate;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileBoundedPredicate;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.nullValue;

/**
 * Checks that {@link GeoGridFromDocValuesBlockLoader} produces, for every document, exactly the cell ids that
 * result from loading the raw encoded {@code geo_point} doc values and encoding them afterwards, for unbounded and
 * bounded grids. Geohash and geotile are used since those libraries are available to the server module; geohex only
 * differs in the encoder, which the loader treats as opaque.
 */
public class GeoGridFromDocValuesBlockLoaderTests extends AbstractNumericBlockLoaderTests {
    private static final int PRECISION = 5;
    private static final BlockLoaderFunctionConfig.GeoGridShapeTilerFactory POINTS_ONLY = warnings -> {
        throw new AssertionError("shapes are not loaded by the point loader");
    };
    /** Roughly Europe and Africa, so a good share of the globally spread test points fall outside. */
    private static final GeoBoundingBox BOUNDS = new GeoBoundingBox(new GeoPoint(60, -20), new GeoPoint(-35, 50));

    public GeoGridFromDocValuesBlockLoaderTests(boolean multiValues, boolean missingValues) {
        super(multiValues, missingValues);
    }

    @Override
    protected IndexableField field(int v) {
        // Spread the seeds over the globe so many distinct cells are produced, including cells shared by neighbouring seeds.
        double lat = -85 + (v * 7919L % 170_000) / 1000.0;
        double lon = -180 + (v * 104_729L % 360_000) / 1000.0;
        return new LatLonDocValuesField("field", lat, lon);
    }

    @Override
    protected void innerTest(CircuitBreaker breaker, LeafReaderContext ctx, int mvCount) throws IOException {
        for (BlockLoaderFunctionConfig.GeoGrid config : List.of(
            geohash(PRECISION, null),
            geotile(PRECISION, null),
            geohash(PRECISION, BOUNDS),
            geotile(PRECISION, BOUNDS)
        )) {
            LongsBlockLoader pointsLoader = new LongsBlockLoader("field");
            GeoGridFromDocValuesBlockLoader gridLoader = new GeoGridFromDocValuesBlockLoader("field", config);
            BlockLoader.Docs docs = TestBlock.docs(ctx);

            try (
                BlockLoader.ColumnAtATimeReader pointsReader = pointsLoader.reader(breaker, ctx);
                BlockLoader.ColumnAtATimeReader gridReader = gridLoader.reader(breaker, ctx)
            ) {
                assertThat(gridReader, readerMatcher());
                try (TestBlock points = read(pointsReader, docs); TestBlock cells = read(gridReader, docs)) {
                    checkBlocks(points, cells, config);
                }
            }

            try (
                BlockLoader.ColumnAtATimeReader pointsReader = pointsLoader.reader(breaker, ctx);
                BlockLoader.ColumnAtATimeReader gridReader = gridLoader.reader(breaker, ctx)
            ) {
                for (int i = 0; i < ctx.reader().numDocs(); i += 10) {
                    int[] docsArray = new int[Math.min(10, ctx.reader().numDocs() - i)];
                    for (int d = 0; d < docsArray.length; d++) {
                        docsArray[d] = i + d;
                    }
                    docs = TestBlock.docs(docsArray);
                    try (TestBlock points = read(pointsReader, docs); TestBlock cells = read(gridReader, docs)) {
                        checkBlocks(points, cells, config);
                    }
                }
            }
        }
    }

    static BlockLoaderFunctionConfig.GeoGrid geohash(int precision, GeoBoundingBox bounds) {
        Supplier<BlockLoaderFunctionConfig.GeoGridEncoder> encoders;
        if (bounds == null) {
            encoders = () -> (lon, lat) -> Geohash.longEncode(lon, lat, precision);
        } else {
            encoders = () -> {
                GeoHashBoundedPredicate predicate = new GeoHashBoundedPredicate(precision, bounds);
                return (lon, lat) -> {
                    String hash = Geohash.stringEncode(lon, lat, precision);
                    return predicate.validHash(hash) ? Geohash.longEncode(hash) : -1;
                };
            };
        }
        return new BlockLoaderFunctionConfig.GeoGrid(
            BlockLoaderFunctionConfig.Function.ST_GEOHASH,
            precision,
            bounds,
            encoders,
            POINTS_ONLY
        );
    }

    static BlockLoaderFunctionConfig.GeoGrid geotile(int precision, GeoBoundingBox bounds) {
        Supplier<BlockLoaderFunctionConfig.GeoGridEncoder> encoders;
        if (bounds == null) {
            encoders = () -> (lon, lat) -> GeoTileUtils.longEncode(lon, lat, precision);
        } else {
            encoders = () -> {
                GeoTileBoundedPredicate predicate = new GeoTileBoundedPredicate(precision, bounds);
                int tiles = 1 << precision;
                return (lon, lat) -> {
                    int x = GeoTileUtils.getXTile(lon, tiles);
                    int y = GeoTileUtils.getYTile(lat, tiles);
                    return predicate.validTile(x, y, precision) ? GeoTileUtils.longEncodeTiles(precision, x, y) : -1;
                };
            };
        }
        return new BlockLoaderFunctionConfig.GeoGrid(
            BlockLoaderFunctionConfig.Function.ST_GEOTILE,
            precision,
            bounds,
            encoders,
            POINTS_ONLY
        );
    }

    private Matcher<Object> readerMatcher() {
        if (multiValues) {
            return hasToString("GeoGridFromDocValues.Sorted");
        }
        return hasToString("GeoGridFromDocValues.Singleton");
    }

    @SuppressWarnings("unchecked")
    private void checkBlocks(TestBlock points, TestBlock cells, BlockLoaderFunctionConfig.GeoGrid config) {
        BlockLoaderFunctionConfig.GeoGridEncoder encoder = config.encoders().get();
        int outOfBounds = 0;
        for (int i = 0; i < points.size(); i++) {
            Object v = points.get(i);
            if (v == null) {
                assertThat(cells.get(i), nullValue());
                continue;
            }
            List<Long> pointValues = v instanceof List<?> l ? (List<Long>) l : List.of((Long) v);
            List<Long> expected = pointValues.stream()
                .map(encoded -> GeoGridFromDocValuesBlockLoader.cellId(encoded, encoder))
                .filter(cellId -> cellId >= 0)
                .toList();
            outOfBounds += pointValues.size() - expected.size();
            if (expected.isEmpty()) {
                assertThat(cells.get(i), nullValue());
            } else if (expected.size() == 1) {
                assertThat(cells.get(i), equalTo(expected.get(0)));
            } else {
                assertThat(cells.get(i), equalTo(expected));
            }
        }
        if (config.bounds() != null && points.size() > 100) {
            assertThat("bounded configs should see some points outside the bounds", outOfBounds, greaterThan(0));
        }
    }
}
