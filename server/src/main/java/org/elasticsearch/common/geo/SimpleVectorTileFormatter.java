/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.geo;

import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;

import java.util.List;
import java.util.Locale;
import java.util.function.Function;

/**
 * A facade for SimpleFeatureFactory that converts it into FormatterFactory for use in GeoPointFieldMapper
 */
public class SimpleVectorTileFormatter implements GeoFormatterFactory.FormatterFactory<GeoPoint> {

    public static final String MVT = "mvt";

    @Override
    public String getName() {
        return MVT;
    }

    @Override
    public Function<String, Function<List<GeoPoint>, List<Object>>> getFormatterBuilder() {
        return  params -> {
            int[] parsed = parse(params);
            final SimpleFeatureFactory featureFactory = new SimpleFeatureFactory(parsed[0], parsed[1], parsed[2], parsed[3]);
            return points -> List.of(featureFactory.points(points));
        };
    }

    /**
     * Parses string in the format we expect either z/x/y or z/x/y@extent to an array of integer parameters
     */
    public static int[] parse(String param) {
        // we expect either z/x/y or z/x/y@extent
        final String[] parts = param.split("@", 3);
        if (parts.length > 2) {
            throw new IllegalArgumentException(
                "Invalid mvt formatter parameter [" + param + "]. Must have the form \"zoom/x/y\" or \"zoom/x/y@extent\"."
            );
        }
        final int extent = parts.length == 2 ? Integer.parseInt(parts[1]) : 4096;
        final String[] tileBits = parts[0].split("/", 4);
        if (tileBits.length != 3) {
            throw new IllegalArgumentException(
                "Invalid tile string [" + parts[0] + "]. Must be three integers in a form \"zoom/x/y\"."
            );
        }
        final int z = GeoTileUtils.checkPrecisionRange(Integer.parseInt(tileBits[0]));
        final int tiles = 1 << z;
        final int x = Integer.parseInt(tileBits[1]);
        final int y = Integer.parseInt(tileBits[2]);
        if (x < 0 || y < 0 || x >= tiles || y >= tiles) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Zoom/X/Y combination is not valid: %d/%d/%d", z, x, y));
        }
        return new int[]{z, x, y, extent};
    }
}
