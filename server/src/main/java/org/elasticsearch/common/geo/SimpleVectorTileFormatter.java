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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A facade for SimpleFeatureFactory that converts it into FormatterFactory for use in GeoPointFieldMapper
 */
public class SimpleVectorTileFormatter implements GeoFormatterFactory.FormatterFactory<GeoPoint> {

    public static final String MVT = "mvt";
    public static final int DEFAULT_EXTENT = 4096;
    public static final int DEFAULT_BUFFER_PIXELS = 5;
    public static final String EXTENT_PREFIX = "@";
    public static final String BUFFER_PREFIX = ":";

    // we expect either z/x/y or z/x/y@extent or z/x/y@extent:buffer or z/x/y:buffer
    private static final Pattern pattern = Pattern.compile("(\\d+)/(\\d+)/(\\d+)(" + EXTENT_PREFIX + "\\d+)?(" + BUFFER_PREFIX + "\\d+)?");

    @Override
    public String getName() {
        return MVT;
    }

    @Override
    public Function<String, Function<List<GeoPoint>, List<Object>>> getFormatterBuilder() {
        return params -> {
            int[] parsed = parse(params);
            final SimpleFeatureFactory featureFactory = new SimpleFeatureFactory(parsed[0], parsed[1], parsed[2], parsed[3]);
            return points -> List.of(featureFactory.points(points));
        };
    }

    /**
     * Parses string in the format we expect, primarily z/x/y, to an array of integer parameters.
     * There are also two optional additions to the format:
     * <ul>
     *     <li><code>@extent</code> - number of pixels across</li>
     *     <li><code>:buffer</code> - number of pixels by which to widen each tile</li>
     * </ul>
     * The resulting array will always have five elements, with the extent and buffer set to the default
     * values if not provided.
     * Examples:
     * <ul>
     *     <li><code>3/2/2</code> produces <code>[3,2,2,4096,5]</code></li>
     *     <li><code>2/1/1@5000</code> produces <code>[2,1,1,5000,5]</code></li>
     *     <li><code>2/1/1@5000:10</code> produces <code>[2,1,1,5000,10]</code></li>
     * </ul>
     */
    public static int[] parse(String param) {
        Matcher matcher = pattern.matcher(param);
        if (matcher.matches() == false) {
            throw new IllegalArgumentException(
                "Invalid mvt formatter parameter ["
                    + param
                    + "]. Must have the form \"zoom/x/y\" or \"zoom/x/y@extent\" or \"zoom/x/y@extent:buffer\" or \"zoom/x/y:buffer\"."
            );
        }
        final int z = GeoTileUtils.checkPrecisionRange(Integer.parseInt(matcher.group(1)));
        final int tiles = 1 << z;
        final int x = Integer.parseInt(matcher.group(2));
        final int y = Integer.parseInt(matcher.group(3));
        if (x < 0 || y < 0 || x >= tiles || y >= tiles) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Zoom/X/Y combination is not valid: %d/%d/%d", z, x, y));
        }
        final int extent = matcher.group(4) == null ? DEFAULT_EXTENT : Integer.parseInt(matcher.group(4).substring(1));
        if (extent <= 0) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Extent is not valid: %d is not > 0", extent));
        }
        final int bufferPixels = matcher.group(5) == null ? DEFAULT_BUFFER_PIXELS : Integer.parseInt(matcher.group(5).substring(1));
        return new int[] { z, x, y, extent, bufferPixels };
    }
}
