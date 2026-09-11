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
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.TestBlock;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.index.mapper.blockloader.docvalues.LongsBlockLoader;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.nullValue;

/**
 * Checks that {@link GeoGridFromDocValuesBlockLoader} produces, for every document, exactly the cell ids that
 * result from loading the raw encoded {@code geo_point} doc values and encoding them afterwards. Geohash and
 * geotile are used since those libraries are available to the server module; geohex only differs in the encoder,
 * which the loader treats as opaque.
 */
public class GeoGridFromDocValuesBlockLoaderTests extends AbstractNumericBlockLoaderTests {
    private static final int PRECISION = 5;

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
        for (BlockLoaderFunctionConfig.GeoGrid config : List.of(geohash(PRECISION), geotile(PRECISION))) {
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

    static BlockLoaderFunctionConfig.GeoGrid geohash(int precision) {
        return new BlockLoaderFunctionConfig.GeoGrid(
            BlockLoaderFunctionConfig.Function.ST_GEOHASH,
            precision,
            (lon, lat) -> Geohash.longEncode(lon, lat, precision)
        );
    }

    static BlockLoaderFunctionConfig.GeoGrid geotile(int precision) {
        return new BlockLoaderFunctionConfig.GeoGrid(
            BlockLoaderFunctionConfig.Function.ST_GEOTILE,
            precision,
            (lon, lat) -> GeoTileUtils.longEncode(lon, lat, precision)
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
        for (int i = 0; i < points.size(); i++) {
            Object v = points.get(i);
            if (v == null) {
                assertThat(cells.get(i), nullValue());
                continue;
            }
            if (v instanceof List<?> l) {
                List<Long> expected = ((List<Long>) l).stream()
                    .map(encoded -> GeoGridFromDocValuesBlockLoader.cellId(encoded, config.encoder()))
                    .toList();
                assertThat(cells.get(i), equalTo(expected));
            } else {
                assertThat(cells.get(i), equalTo(GeoGridFromDocValuesBlockLoader.cellId((Long) v, config.encoder())));
            }
        }
    }
}
