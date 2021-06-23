/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectortile;

import org.elasticsearch.common.geo.GeoFormatterFactory;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.vectortile.feature.FeatureFactory;
import org.hamcrest.Matchers;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class GeoFormatterExtensionTests extends ESTestCase {

    public void testBasicFormat() {
        final int z = randomIntBetween(1, GeoTileUtils.MAX_ZOOM);
        final int x = randomIntBetween(0, (1 << z) - 1);
        final int y = randomIntBetween(0, (1 << z) - 1);
        final int extent = randomIntBetween(1024, 8012);

        final Map<String, GeoFormatterFactory.GeoFormatterEngine> formatters = new VectorTilePlugin().getGeoFormatters();
        final GeoFormatterFactory.GeoFormatterEngine engine = formatters.get("mvt");
        assertThat(engine, Matchers.notNullValue());
        final Function<List<Geometry>, List<Object>> formatter = engine.getFormatter(z + "/" + x + "/" + y + "@" + extent);
        final FeatureFactory factory = new FeatureFactory(z, x, y, extent);

        final List<Geometry> geometry = List.of(GeoTileUtils.toBoundingBox(x, y, z));
        assertThat(formatter.apply(geometry).get(0), Matchers.equalTo(factory.apply(geometry).get(0)));
    }

    public void testDefaultExtentFormat() {
        final int z = randomIntBetween(1, GeoTileUtils.MAX_ZOOM);
        final int x = randomIntBetween(0, (1 << z) - 1);
        final int y = randomIntBetween(0, (1 << z) - 1);

        final Map<String, GeoFormatterFactory.GeoFormatterEngine> formatters = new VectorTilePlugin().getGeoFormatters();
        final GeoFormatterFactory.GeoFormatterEngine engine = formatters.get("mvt");
        assertThat(engine, Matchers.notNullValue());
        final Function<List<Geometry>, List<Object>> formatter = engine.getFormatter(z + "/" + x + "/" + y);
        final FeatureFactory factory = new FeatureFactory(z, x, y, 4096);

        final List<Geometry> geometry = List.of(GeoTileUtils.toBoundingBox(x, y, z));
        assertThat(formatter.apply(geometry).get(0), Matchers.equalTo(factory.apply(geometry).get(0)));
    }

    public void testBasicInvalidTile() {
        final Map<String, GeoFormatterFactory.GeoFormatterEngine> formatters = new VectorTilePlugin().getGeoFormatters();
        final GeoFormatterFactory.GeoFormatterEngine engine = formatters.get("mvt");
        assertThat(engine, Matchers.notNullValue());
        {
            // negative zoom
            final int z = randomIntBetween(Integer.MIN_VALUE, -1);
            final int x = 0;
            final int y = 0;
            final IllegalArgumentException ex = expectThrows(
                IllegalArgumentException.class,
                () -> engine.getFormatter(z + "/" + x + "/" + y)
            );
            assertThat(ex.getMessage(), Matchers.equalTo("Invalid geotile_grid precision of " + z + ". Must be between 0 and 29."));
        }
        {
            // too big zoom
            final int z = randomIntBetween(GeoTileUtils.MAX_ZOOM + 1, Integer.MAX_VALUE);
            final int x = 0;
            final int y = 0;
            final IllegalArgumentException ex = expectThrows(
                IllegalArgumentException.class,
                () -> engine.getFormatter(z + "/" + x + "/" + y)
            );
            assertThat(ex.getMessage(), Matchers.equalTo("Invalid geotile_grid precision of " + z + ". Must be between 0 and 29."));
        }
        {
            // negative x
            final int z = randomIntBetween(0, GeoTileUtils.MAX_ZOOM);
            final int x = randomIntBetween(Integer.MIN_VALUE, -1);
            final int y = randomIntBetween(0, (1 << z) - 1);
            final IllegalArgumentException ex = expectThrows(
                IllegalArgumentException.class,
                () -> engine.getFormatter(z + "/" + x + "/" + y)
            );
            assertThat(ex.getMessage(), Matchers.equalTo("Zoom/X/Y combination is not valid: " + z + "/" + x + "/" + y));
        }
        {
            // too big x
            final int z = randomIntBetween(0, GeoTileUtils.MAX_ZOOM);
            final int x = randomIntBetween(Integer.MIN_VALUE, -1);
            final int y = randomIntBetween(1 << z, Integer.MAX_VALUE);
            final IllegalArgumentException ex = expectThrows(
                IllegalArgumentException.class,
                () -> engine.getFormatter(z + "/" + x + "/" + y)
            );
            assertThat(ex.getMessage(), Matchers.equalTo("Zoom/X/Y combination is not valid: " + z + "/" + x + "/" + y));
        }
        {
            // negative y
            final int z = randomIntBetween(0, GeoTileUtils.MAX_ZOOM);
            final int x = randomIntBetween(0, (1 << z) - 1);
            final int y = randomIntBetween(Integer.MIN_VALUE, -1);
            final IllegalArgumentException ex = expectThrows(
                IllegalArgumentException.class,
                () -> engine.getFormatter(z + "/" + x + "/" + y)
            );
            assertThat(ex.getMessage(), Matchers.equalTo("Zoom/X/Y combination is not valid: " + z + "/" + x + "/" + y));
        }
        {
            // too big y
            final int z = randomIntBetween(0, GeoTileUtils.MAX_ZOOM);
            final int x = randomIntBetween(1 << z, Integer.MAX_VALUE);
            final int y = randomIntBetween(Integer.MIN_VALUE, -1);
            final IllegalArgumentException ex = expectThrows(
                IllegalArgumentException.class,
                () -> engine.getFormatter(z + "/" + x + "/" + y)
            );
            assertThat(ex.getMessage(), Matchers.equalTo("Zoom/X/Y combination is not valid: " + z + "/" + x + "/" + y));
        }
    }
}
