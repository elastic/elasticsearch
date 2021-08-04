/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.geo;

import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;

import java.util.List;
import java.util.Locale;
import java.util.function.Function;

/**
 * Output formatters for geo fields. Adds support for vector tiles.
 */
public class GeoFormatterFactory {

    @FunctionalInterface
    public interface VectorTileEngine<T>  {
        /**
         * Returns a formatter for a specific tile.
         */
        Function<List<T>, List<Object>> getFormatter(int z, int x, int y, int extent);
    }

    private static final String MVT = "mvt";

    /**
     * Returns a formatter by name
     */
    public static <T> Function<List<T>, List<Object>> getFormatter(String format, Function<T, Geometry> toGeometry,
                                                                          VectorTileEngine<T> mvt) {
        final int start = format.indexOf('(');
        if (start == -1)  {
            return GeometryFormatterFactory.getFormatter(format, toGeometry);
        }
        final String formatName = format.substring(0, start);
        if (MVT.equals(formatName) == false) {
            throw new IllegalArgumentException("Invalid format: " + formatName);
        }
        final String param = format.substring(start + 1, format.length() - 1);
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
        return mvt.getFormatter(z, x, y, extent);
    }
}
