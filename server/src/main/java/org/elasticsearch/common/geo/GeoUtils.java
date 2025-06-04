/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.geo;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.util.SloppyMath;
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.GeoPointValues;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.fielddata.SortingNumericDoubleValues;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.support.MapXContentParser;

import java.io.IOException;
import java.util.Collections;

public class GeoUtils {

    /** Maximum valid latitude in degrees. */
    public static final double MAX_LAT = 90.0;
    /** Minimum valid latitude in degrees. */
    public static final double MIN_LAT = -90.0;
    /** Maximum valid longitude in degrees. */
    public static final double MAX_LON = 180.0;
    /** Minimum valid longitude in degrees. */
    public static final double MIN_LON = -180.0;

    /** Earth ellipsoid major axis defined by WGS 84 in meters */
    public static final double EARTH_SEMI_MAJOR_AXIS = 6378137.0;      // meters (WGS 84)

    /** Earth ellipsoid minor axis defined by WGS 84 in meters */
    public static final double EARTH_SEMI_MINOR_AXIS = 6356752.314245; // meters (WGS 84)

    /** Earth mean radius defined by WGS 84 in meters */
    public static final double EARTH_MEAN_RADIUS = 6371008.7714D;      // meters (WGS 84)

    /** Earth ellipsoid equator length in meters */
    public static final double EARTH_EQUATOR = 2 * Math.PI * EARTH_SEMI_MAJOR_AXIS;

    /** Earth ellipsoid polar distance in meters */
    public static final double EARTH_POLAR_DISTANCE = Math.PI * EARTH_SEMI_MINOR_AXIS;

    /** rounding error for quantized latitude and longitude values */
    public static final double TOLERANCE = 1E-6;

    private static final int QUAD_MAX_LEVELS_POSSIBLE = 50;
    private static final int GEOHASH_MAX_LEVELS_POSSIBLE = 24;

    /** Returns true if latitude is actually a valid latitude value.*/
    public static boolean isValidLatitude(double latitude) {
        if (Double.isNaN(latitude) || Double.isInfinite(latitude) || latitude < GeoUtils.MIN_LAT || latitude > GeoUtils.MAX_LAT) {
            return false;
        }
        return true;
    }

    /** Returns true if longitude is actually a valid longitude value. */
    public static boolean isValidLongitude(double longitude) {
        if (Double.isNaN(longitude) || Double.isInfinite(longitude) || longitude < GeoUtils.MIN_LON || longitude > GeoUtils.MAX_LON) {
            return false;
        }
        return true;
    }

    /**
     * Calculate the width (in meters) of geohash cells at a specific level
     * @param level geohash level must be greater or equal to zero
     * @return the width of cells at level in meters
     */
    public static double geoHashCellWidth(int level) {
        assert level >= 0;
        // Geohash cells are split into 32 cells at each level. the grid
        // alternates at each level between a 8x4 and a 4x8 grid
        return EARTH_EQUATOR / (1L << ((((level + 1) / 2) * 3) + ((level / 2) * 2)));
    }

    /**
     * Calculate the width (in meters) of quadtree cells at a specific level
     * @param level quadtree level must be greater or equal to zero
     * @return the width of cells at level in meters
     */
    public static double quadTreeCellWidth(int level) {
        assert level >= 0;
        return EARTH_EQUATOR / (1L << level);
    }

    /**
     * Calculate the height (in meters) of geohash cells at a specific level
     * @param level geohash level must be greater or equal to zero
     * @return the height of cells at level in meters
     */
    public static double geoHashCellHeight(int level) {
        assert level >= 0;
        // Geohash cells are split into 32 cells at each level. the grid
        // alternates at each level between a 8x4 and a 4x8 grid
        return EARTH_POLAR_DISTANCE / (1L << ((((level + 1) / 2) * 2) + ((level / 2) * 3)));
    }

    /**
     * Calculate the height (in meters) of quadtree cells at a specific level
     * @param level quadtree level must be greater or equal to zero
     * @return the height of cells at level in meters
     */
    public static double quadTreeCellHeight(int level) {
        assert level >= 0;
        return EARTH_POLAR_DISTANCE / (1L << level);
    }

    /**
     * Calculate the size (in meters) of geohash cells at a specific level
     * @param level geohash level must be greater or equal to zero
     * @return the size of cells at level in meters
     */
    public static double geoHashCellSize(int level) {
        assert level >= 0;
        final double w = geoHashCellWidth(level);
        final double h = geoHashCellHeight(level);
        return Math.sqrt(w * w + h * h);
    }

    /**
     * Calculate the size (in meters) of quadtree cells at a specific level
     * @param level quadtree level must be greater or equal to zero
     * @return the size of cells at level in meters
     */
    public static double quadTreeCellSize(int level) {
        assert level >= 0;
        return Math.sqrt(EARTH_POLAR_DISTANCE * EARTH_POLAR_DISTANCE + EARTH_EQUATOR * EARTH_EQUATOR) / (1L << level);
    }

    /**
     * Calculate the number of levels needed for a specific precision. Quadtree
     * cells will not exceed the specified size (diagonal) of the precision.
     * @param meters Maximum size of cells in meters (must greater than zero)
     * @return levels need to achieve precision
     */
    public static int quadTreeLevelsForPrecision(double meters) {
        assert meters >= 0;
        if (meters == 0) {
            return QUAD_MAX_LEVELS_POSSIBLE;
        } else {
            final double ratio = 1 + (EARTH_POLAR_DISTANCE / EARTH_EQUATOR); // cell ratio
            final double width = Math.sqrt((meters * meters) / (ratio * ratio)); // convert to cell width
            final long part = Math.round(Math.ceil(EARTH_EQUATOR / width));
            final int level = Long.SIZE - Long.numberOfLeadingZeros(part) - 1; // (log_2)
            return (part <= (1L << level)) ? level : (level + 1); // adjust level
        }
    }

    /**
     * Calculate the number of levels needed for a specific precision. QuadTree
     * cells will not exceed the specified size (diagonal) of the precision.
     * @param distance Maximum size of cells as unit string (must greater or equal to zero)
     * @return levels need to achieve precision
     */
    public static int quadTreeLevelsForPrecision(String distance) {
        return quadTreeLevelsForPrecision(DistanceUnit.METERS.parse(distance, DistanceUnit.DEFAULT));
    }

    /**
     * Calculate the number of levels needed for a specific precision. GeoHash
     * cells will not exceed the specified size (diagonal) of the precision.
     * @param meters Maximum size of cells in meters (must greater or equal to zero)
     * @return levels need to achieve precision
     */
    public static int geoHashLevelsForPrecision(double meters) {
        assert meters >= 0;

        if (meters == 0) {
            return GEOHASH_MAX_LEVELS_POSSIBLE;
        } else {
            final double ratio = 1 + (EARTH_POLAR_DISTANCE / EARTH_EQUATOR); // cell ratio
            final double width = Math.sqrt((meters * meters) / (ratio * ratio)); // convert to cell width
            final double part = Math.ceil(EARTH_EQUATOR / width);
            if (part == 1) return 1;
            final int bits = (int) Math.round(Math.ceil(Math.log(part) / Math.log(2)));
            final int full = bits / 5;                // number of 5 bit subdivisions
            final int left = bits - full * 5;           // bit representing the last level
            final int even = full + (left > 0 ? 1 : 0);     // number of even levels
            final int odd = full + (left > 3 ? 1 : 0);      // number of odd levels
            return even + odd;
        }
    }

    /**
     * Calculate the number of levels needed for a specific precision. GeoHash
     * cells will not exceed the specified size (diagonal) of the precision.
     * @param distance Maximum size of cells as unit string (must greater or equal to zero)
     * @return levels need to achieve precision
     */
    public static int geoHashLevelsForPrecision(String distance) {
        return geoHashLevelsForPrecision(DistanceUnit.METERS.parse(distance, DistanceUnit.DEFAULT));
    }

    /**
     * Normalize longitude to lie within the -180 (exclusive) to 180 (inclusive) range.
     *
     * @param lon Longitude to normalize
     * @return The normalized longitude.
     */
    public static double normalizeLon(double lon) {
        if (lon > 180d || lon <= -180d) {
            lon = centeredModulus(lon, 360);
        }
        // avoid -0.0
        return lon + 0d;
    }

    /**
     * Normalize latitude to lie within the -90 to 90 (both inclusive) range.
     * <p>
     * Note: You should not normalize longitude and latitude separately,
     * because when normalizing latitude it may be necessary to
     * add a shift of 180&deg; in the longitude.
     * For this purpose, you should call the
     * {@link #normalizePoint(GeoPoint)} function.
     *
     * @param lat Latitude to normalize
     * @return The normalized latitude.
     * @see #normalizePoint(GeoPoint)
     */
    public static double normalizeLat(double lat) {
        if (lat > 90d || lat < -90d) {
            lat = centeredModulus(lat, 360);
            if (lat < -90) {
                lat = -180 - lat;
            } else if (lat > 90) {
                lat = 180 - lat;
            }
        }
        // avoid -0.0
        return lat + 0d;
    }

    /**
     * Normalize the geo {@code Point} for its coordinates to lie within their
     * respective normalized ranges.
     * <p>
     * Note: A shift of 180&deg; is applied in the longitude if necessary,
     * in order to normalize properly the latitude.
     *
     * @param point The point to normalize in-place.
     */
    public static void normalizePoint(GeoPoint point) {
        normalizePoint(point, true, true);
    }

    /**
     * Normalize the geo {@code Point} for the given coordinates to lie within
     * their respective normalized ranges.
     * <p>
     * You can control which coordinate gets normalized with the two flags.
     * <p>
     * Note: A shift of 180&deg; is applied in the longitude if necessary,
     * in order to normalize properly the latitude.
     * If normalizing latitude but not longitude, it is assumed that
     * the longitude is in the form x+k*360, with x in ]-180;180],
     * and k is meaningful to the application.
     * Therefore x will be adjusted while keeping k preserved.
     *
     * @param point   The point to normalize in-place.
     * @param normLat Whether to normalize latitude or leave it as is.
     * @param normLon Whether to normalize longitude.
     */
    public static void normalizePoint(GeoPoint point, boolean normLat, boolean normLon) {
        double[] pt = { point.lon(), point.lat() };
        normalizePoint(pt, normLon, normLat);
        point.reset(pt[1], pt[0]);
    }

    public static void normalizePoint(double[] lonLat) {
        normalizePoint(lonLat, true, true);
    }

    public static boolean needsNormalizeLat(double lat) {
        return lat > 90 || lat < -90;
    }

    public static boolean needsNormalizeLon(double lon) {
        return lon > 180 || lon < -180;
    }

    public static void normalizePoint(double[] lonLat, boolean normLon, boolean normLat) {
        assert lonLat != null && lonLat.length == 2;

        normLat = normLat && needsNormalizeLat(lonLat[1]);
        normLon = normLon && (needsNormalizeLon(lonLat[0]) || normLat);

        if (normLat) {
            lonLat[1] = centeredModulus(lonLat[1], 360);
            boolean shift = true;
            if (lonLat[1] < -90) {
                lonLat[1] = -180 - lonLat[1];
            } else if (lonLat[1] > 90) {
                lonLat[1] = 180 - lonLat[1];
            } else {
                // No need to shift the longitude, and the latitude is normalized
                shift = false;
            }
            if (shift) {
                if (normLon) {
                    lonLat[0] += 180;
                } else {
                    // Longitude won't be normalized,
                    // keep it in the form x+k*360 (with x in ]-180;180])
                    // by only changing x, assuming k is meaningful for the user application.
                    lonLat[0] += normalizeLon(lonLat[0]) > 0 ? -180 : 180;
                }
            }
        }
        if (normLon) {
            lonLat[0] = centeredModulus(lonLat[0], 360);
        }
    }

    public static double centeredModulus(double dividend, double divisor) {
        double rtn = dividend % divisor;
        if (rtn <= 0) {
            rtn += divisor;
        }
        if (rtn > divisor / 2) {
            rtn -= divisor;
        }
        return rtn;
    }

    /**
     * Parse a {@link GeoPoint} with a {@link XContentParser}:
     *
     * @param parser {@link XContentParser} to parse the value from
     * @return new {@link GeoPoint} parsed from the parse
     */
    public static GeoPoint parseGeoPoint(XContentParser parser) throws IOException, ElasticsearchParseException {
        return parseGeoPoint(parser, false);
    }

    /**
     * Parses the value as a geopoint. The following types of values are supported:
     * <p>
     * Object: has to contain either lat and lon or geohash or type and coordinates fields
     * <p>
     * String: expected to be in "latitude, longitude" format or a geohash
     * <p>
     * Array: two or more elements, the first element is longitude, the second is latitude, the rest is ignored if ignoreZValue is true
     */
    public static GeoPoint parseGeoPoint(Object value, final boolean ignoreZValue) throws ElasticsearchParseException {
        try (
            XContentParser parser = new MapXContentParser(
                NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE,
                Collections.singletonMap("null_value", value),
                null
            )
        ) {
            parser.nextToken(); // start object
            parser.nextToken(); // field name
            parser.nextToken(); // field value
            return parseGeoPoint(parser, ignoreZValue);
        } catch (IOException ex) {
            throw new ElasticsearchParseException("error parsing geopoint", ex);
        }
    }

    /**
     * Represents the point of the geohash cell that should be used as the value of geohash
     */
    public enum EffectivePoint {
        TOP_LEFT,
        TOP_RIGHT,
        BOTTOM_LEFT,
        BOTTOM_RIGHT
    }

    /**
     * Parse a geopoint represented as an object, string or an array. If the geopoint is represented as a geohash,
     * the left bottom corner of the geohash cell is used as the geopoint coordinates.GeoBoundingBoxQueryBuilder.java
     */
    public static GeoPoint parseGeoPoint(XContentParser parser, final boolean ignoreZValue) throws IOException,
        ElasticsearchParseException {
        return parseGeoPoint(parser, ignoreZValue, EffectivePoint.BOTTOM_LEFT);
    }

    /**
     * Parse a {@link GeoPoint} with a {@link XContentParser}. A geo_point has one of the following forms:
     *
     * <ul>
     *     <li>Object: <pre>{&quot;lat&quot;: <i>&lt;latitude&gt;</i>, &quot;lon&quot;: <i>&lt;longitude&gt;</i>}</pre></li>
     *     <li>Object: <pre>{&quot;type&quot;: <i>Point</i>, &quot;coordinates&quot;: <i>&lt;array of doubles&gt;</i>}</pre></li>
     *     <li>String: <pre>&quot;<i>&lt;latitude&gt;</i>,<i>&lt;longitude&gt;</i>&quot;</pre></li>
     *     <li>Geohash: <pre>&quot;<i>&lt;geohash&gt;</i>&quot;</pre></li>
     *     <li>Array: <pre>[<i>&lt;longitude&gt;</i>,<i>&lt;latitude&gt;</i>]</pre></li>
     * </ul>
     *
     * @param parser {@link XContentParser} to parse the value from
     * @param ignoreZValue {@link XContentParser} to not throw an error if 3 dimensional data is provided
     * @return new {@link GeoPoint} parsed from the parse
     */
    public static GeoPoint parseGeoPoint(XContentParser parser, final boolean ignoreZValue, final EffectivePoint effectivePoint)
        throws IOException, ElasticsearchParseException {
        return geoPointParser.parsePoint(
            parser,
            ignoreZValue,
            value -> new GeoPoint().resetFromString(value, ignoreZValue, effectivePoint)
        );
    }

    private static final GenericPointParser<GeoPoint> geoPointParser = new GenericPointParser<>("geo_point", "lon", "lat") {

        @Override
        public void assertZValue(boolean ignoreZValue, double zValue) {
            GeoPoint.assertZValue(ignoreZValue, zValue);
        }

        @Override
        public GeoPoint createPoint(double x, double y) {
            // GeoPoint takes lat,lon which is the reverse order from CartesianPoint
            return new GeoPoint(y, x);
        }

        @Override
        public String fieldError() {
            return "field must be either lat/lon, geohash string or type/coordinates";
        }
    };

    /**
     * Parse a {@link GeoPoint} from a string. The string must have one of the following forms:
     *
     * <ul>
     *     <li>Latitude, Longitude form: <pre>&quot;<i>&lt;latitude&gt;</i>,<i>&lt;longitude&gt;</i>&quot;</pre></li>
     *     <li>Geohash form:: <pre>&quot;<i>&lt;geohash&gt;</i>&quot;</pre></li>
     * </ul>
     *
     * @param val a String to parse the value from
     * @return new parsed {@link GeoPoint}
     */
    public static GeoPoint parseFromString(String val) {
        GeoPoint point = new GeoPoint();
        return point.resetFromString(val, false, EffectivePoint.BOTTOM_LEFT);
    }

    /**
     * Parse a precision that can be expressed as an integer or a distance measure like "1km", "10m".
     *
     * The precision is expressed as a number between 1 and 12 and indicates the length of geohash
     * used to represent geo points.
     *
     * @param parser {@link XContentParser} to parse the value from
     * @return int representing precision
     */
    public static int parsePrecision(XContentParser parser) throws IOException, ElasticsearchParseException {
        XContentParser.Token token = parser.currentToken();
        if (token.equals(XContentParser.Token.VALUE_NUMBER)) {
            return XContentMapValues.nodeIntegerValue(parser.intValue());
        } else {
            String precision = parser.text();
            try {
                // we want to treat simple integer strings as precision levels, not distances
                return XContentMapValues.nodeIntegerValue(precision);
            } catch (NumberFormatException e) {
                // try to parse as a distance value
                final int parsedPrecision = GeoUtils.geoHashLevelsForPrecision(precision);
                try {
                    return checkPrecisionRange(parsedPrecision);
                } catch (IllegalArgumentException e2) {
                    // this happens when distance too small, so precision > 12. We'd like to see the original string
                    throw new IllegalArgumentException("precision too high [" + precision + "]", e2);
                }
            }
        }
    }

    /**
     * Checks that the precision is within range supported by elasticsearch - between 1 and 12
     *
     * Returns the precision value if it is in the range and throws an IllegalArgumentException if it
     * is outside the range.
     */
    public static int checkPrecisionRange(int precision) {
        if ((precision < 1) || (precision > 12)) {
            throw new IllegalArgumentException("Invalid geohash aggregation precision of " + precision + ". Must be between 1 and 12.");
        }
        return precision;
    }

    /** Return the distance (in meters) between 2 lat,lon geo points using the haversine method implemented by lucene */
    public static double arcDistance(double lat1, double lon1, double lat2, double lon2) {
        return SloppyMath.haversinMeters(lat1, lon1, lat2, lon2);
    }

    /**
     * Return the distance (in meters) between 2 lat,lon geo points using a simple tangential plane
     * this provides a faster alternative to {@link GeoUtils#arcDistance} but is inaccurate for distances greater than
     * 4 decimal degrees
     */
    public static double planeDistance(double lat1, double lon1, double lat2, double lon2) {
        double x = Math.toRadians(lon2 - lon1) * Math.cos(Math.toRadians((lat2 + lat1) / 2.0));
        double y = Math.toRadians(lat2 - lat1);
        return Math.sqrt(x * x + y * y) * EARTH_MEAN_RADIUS;
    }

    /**
     * Return a {@link SortedNumericDoubleValues} instance that returns the distances to a list of geo-points
     * for each document.
     */
    public static SortedNumericDoubleValues distanceValues(
        final GeoDistance distance,
        final DistanceUnit unit,
        final MultiGeoPointValues geoPointValues,
        final GeoPoint... fromPoints
    ) {
        final GeoPointValues singleValues = FieldData.unwrapSingleton(geoPointValues);
        if (singleValues != null && fromPoints.length == 1) {
            return FieldData.singleton(new NumericDoubleValues() {

                @Override
                public boolean advanceExact(int doc) throws IOException {
                    return singleValues.advanceExact(doc);
                }

                @Override
                public double doubleValue() throws IOException {
                    final GeoPoint from = fromPoints[0];
                    final GeoPoint to = singleValues.pointValue();
                    return distance.calculate(from.lat(), from.lon(), to.lat(), to.lon(), unit);
                }

            });
        } else {
            return new SortingNumericDoubleValues() {
                @Override
                public boolean advanceExact(int target) throws IOException {
                    if (geoPointValues.advanceExact(target)) {
                        resize(geoPointValues.docValueCount() * fromPoints.length);
                        int v = 0;
                        for (int i = 0; i < geoPointValues.docValueCount(); ++i) {
                            final GeoPoint point = geoPointValues.nextValue();
                            for (GeoPoint from : fromPoints) {
                                values[v] = distance.calculate(from.lat(), from.lon(), point.lat(), point.lon(), unit);
                                v++;
                            }
                        }
                        sort();
                        return true;
                    } else {
                        return false;
                    }
                }
            };
        }
    }

    /**
     * Transforms the provided longitude to the equivalent in lucene quantize space.
     */
    public static double quantizeLon(double lon) {
        return GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(lon));
    }

    /**
     * Transforms the provided latitude to the equivalent in lucene quantize space.
     */
    public static double quantizeLat(double lat) {
        return GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(lat));
    }

    /**
     * Transforms the provided longitude to the previous longitude in lucene quantize space.
     */
    public static double quantizeLonDown(double lon) {
        return GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(lon) - 1);
    }

    /**
     * Transforms the provided latitude to the next latitude in lucene quantize space.
     */
    public static double quantizeLatUp(double lat) {
        return GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(lat) + 1);
    }

    private GeoUtils() {}
}
