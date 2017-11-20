/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.geo;

import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.QuadPrefixTree;
import org.apache.lucene.util.SloppyMath;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.GeoPointValues;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.fielddata.SortingNumericDoubleValues;

import java.io.IOException;

public class GeoUtils {

    /** Maximum valid latitude in degrees. */
    public static final double MAX_LAT = 90.0;
    /** Minimum valid latitude in degrees. */
    public static final double MIN_LAT = -90.0;
    /** Maximum valid longitude in degrees. */
    public static final double MAX_LON = 180.0;
    /** Minimum valid longitude in degrees. */
    public static final double MIN_LON = -180.0;

    public static final String LATITUDE = "lat";
    public static final String LONGITUDE = "lon";
    public static final String GEOHASH = "geohash";

    /** Earth ellipsoid major axis defined by WGS 84 in meters */
    public static final double EARTH_SEMI_MAJOR_AXIS = 6378137.0;      // meters (WGS 84)

    /** Earth ellipsoid minor axis defined by WGS 84 in meters */
    public static final double EARTH_SEMI_MINOR_AXIS = 6356752.314245; // meters (WGS 84)

    /** Earth mean radius defined by WGS 84 in meters */
    public static final double EARTH_MEAN_RADIUS = 6371008.7714D;      // meters (WGS 84)

    /** Earth axis ratio defined by WGS 84 (0.996647189335) */
    public static final double EARTH_AXIS_RATIO = EARTH_SEMI_MINOR_AXIS / EARTH_SEMI_MAJOR_AXIS;

    /** Earth ellipsoid equator length in meters */
    public static final double EARTH_EQUATOR = 2*Math.PI * EARTH_SEMI_MAJOR_AXIS;

    /** Earth ellipsoid polar distance in meters */
    public static final double EARTH_POLAR_DISTANCE = Math.PI * EARTH_SEMI_MINOR_AXIS;

    /** rounding error for quantized latitude and longitude values */
    public static final double TOLERANCE = 1E-6;

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
        assert level>=0;
        // Geohash cells are split into 32 cells at each level. the grid
        // alternates at each level between a 8x4 and a 4x8 grid
        return EARTH_EQUATOR / (1L<<((((level+1)/2)*3) + ((level/2)*2)));
    }

    /**
     * Calculate the width (in meters) of quadtree cells at a specific level
     * @param level quadtree level must be greater or equal to zero
     * @return the width of cells at level in meters
     */
    public static double quadTreeCellWidth(int level) {
        assert level >=0;
        return EARTH_EQUATOR / (1L<<level);
    }

    /**
     * Calculate the height (in meters) of geohash cells at a specific level
     * @param level geohash level must be greater or equal to zero
     * @return the height of cells at level in meters
     */
    public static double geoHashCellHeight(int level) {
        assert level>=0;
        // Geohash cells are split into 32 cells at each level. the grid
        // alternates at each level between a 8x4 and a 4x8 grid
        return EARTH_POLAR_DISTANCE / (1L<<((((level+1)/2)*2) + ((level/2)*3)));
    }

    /**
     * Calculate the height (in meters) of quadtree cells at a specific level
     * @param level quadtree level must be greater or equal to zero
     * @return the height of cells at level in meters
     */
    public static double quadTreeCellHeight(int level) {
        assert level>=0;
        return EARTH_POLAR_DISTANCE / (1L<<level);
    }

    /**
     * Calculate the size (in meters) of geohash cells at a specific level
     * @param level geohash level must be greater or equal to zero
     * @return the size of cells at level in meters
     */
    public static double geoHashCellSize(int level) {
        assert level>=0;
        final double w = geoHashCellWidth(level);
        final double h = geoHashCellHeight(level);
        return Math.sqrt(w*w + h*h);
    }

    /**
     * Calculate the size (in meters) of quadtree cells at a specific level
     * @param level quadtree level must be greater or equal to zero
     * @return the size of cells at level in meters
     */
    public static double quadTreeCellSize(int level) {
        assert level>=0;
        return Math.sqrt(EARTH_POLAR_DISTANCE*EARTH_POLAR_DISTANCE + EARTH_EQUATOR*EARTH_EQUATOR) / (1L<<level);
    }

    /**
     * Calculate the number of levels needed for a specific precision. Quadtree
     * cells will not exceed the specified size (diagonal) of the precision.
     * @param meters Maximum size of cells in meters (must greater than zero)
     * @return levels need to achieve precision
     */
    public static int quadTreeLevelsForPrecision(double meters) {
        assert meters >= 0;
        if(meters == 0) {
            return QuadPrefixTree.MAX_LEVELS_POSSIBLE;
        } else {
            final double ratio = 1+(EARTH_POLAR_DISTANCE / EARTH_EQUATOR); // cell ratio
            final double width = Math.sqrt((meters*meters)/(ratio*ratio)); // convert to cell width
            final long part = Math.round(Math.ceil(EARTH_EQUATOR / width));
            final int level = Long.SIZE - Long.numberOfLeadingZeros(part)-1; // (log_2)
            return (part<=(1L<<level)) ?level :(level+1); // adjust level
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

        if(meters == 0) {
            return GeohashPrefixTree.getMaxLevelsPossible();
        } else {
            final double ratio = 1+(EARTH_POLAR_DISTANCE / EARTH_EQUATOR); // cell ratio
            final double width = Math.sqrt((meters*meters)/(ratio*ratio)); // convert to cell width
            final double part = Math.ceil(EARTH_EQUATOR / width);
            if(part == 1)
                return 1;
            final int bits = (int)Math.round(Math.ceil(Math.log(part) / Math.log(2)));
            final int full = bits / 5;                // number of 5 bit subdivisions
            final int left = bits - full*5;           // bit representing the last level
            final int even = full + (left>0?1:0);     // number of even levels
            final int odd = full + (left>3?1:0);      // number of odd levels
            return even+odd;
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
        return centeredModulus(lon, 360);
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
        lat = centeredModulus(lat, 360);
        if (lat < -90) {
            lat = -180 - lat;
        } else if (lat > 90) {
            lat = 180 - lat;
        }
        return lat;
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
        double[] pt = {point.lon(), point.lat()};
        normalizePoint(pt, normLon, normLat);
        point.reset(pt[1], pt[0]);
    }

    public static void normalizePoint(double[] lonLat) {
        normalizePoint(lonLat, true, true);
    }

    public static void normalizePoint(double[] lonLat, boolean normLon, boolean normLat) {
        assert lonLat != null && lonLat.length == 2;

        normLat = normLat && (lonLat[1] > 90 || lonLat[1] < -90);
        normLon = normLon && (lonLat[0] > 180 || lonLat[0] < -180);

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

    private static double centeredModulus(double dividend, double divisor) {
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
        return parseGeoPoint(parser, new GeoPoint());
    }

    /**
     * Parse a {@link GeoPoint} with a {@link XContentParser}. A geopoint has one of the following forms:
     *
     * <ul>
     *     <li>Object: <pre>{&quot;lat&quot;: <i>&lt;latitude&gt;</i>, &quot;lon&quot;: <i>&lt;longitude&gt;</i>}</pre></li>
     *     <li>String: <pre>&quot;<i>&lt;latitude&gt;</i>,<i>&lt;longitude&gt;</i>&quot;</pre></li>
     *     <li>Geohash: <pre>&quot;<i>&lt;geohash&gt;</i>&quot;</pre></li>
     *     <li>Array: <pre>[<i>&lt;longitude&gt;</i>,<i>&lt;latitude&gt;</i>]</pre></li>
     * </ul>
     *
     * @param parser {@link XContentParser} to parse the value from
     * @param point A {@link GeoPoint} that will be reset by the values parsed
     * @return new {@link GeoPoint} parsed from the parse
     */
    public static GeoPoint parseGeoPoint(XContentParser parser, GeoPoint point) throws IOException, ElasticsearchParseException {
        double lat = Double.NaN;
        double lon = Double.NaN;
        String geohash = null;
        NumberFormatException numberFormatException = null;

        if(parser.currentToken() == Token.START_OBJECT) {
            while(parser.nextToken() != Token.END_OBJECT) {
                if(parser.currentToken() == Token.FIELD_NAME) {
                    String field = parser.currentName();
                    if(LATITUDE.equals(field)) {
                        parser.nextToken();
                        switch (parser.currentToken()) {
                            case VALUE_NUMBER:
                            case VALUE_STRING:
                                try {
                                    lat = parser.doubleValue(true);
                                } catch (NumberFormatException e) {
                                    numberFormatException = e;
                                }
                                break;
                            default:
                                throw new ElasticsearchParseException("latitude must be a number");
                        }
                    } else if (LONGITUDE.equals(field)) {
                        parser.nextToken();
                        switch (parser.currentToken()) {
                            case VALUE_NUMBER:
                            case VALUE_STRING:
                                try {
                                    lon = parser.doubleValue(true);
                                } catch (NumberFormatException e) {
                                    numberFormatException = e;
                                }
                                break;
                            default:
                                throw new ElasticsearchParseException("longitude must be a number");
                        }
                    } else if (GEOHASH.equals(field)) {
                        if(parser.nextToken() == Token.VALUE_STRING) {
                            geohash = parser.text();
                        } else {
                            throw new ElasticsearchParseException("geohash must be a string");
                        }
                    } else {
                        throw new ElasticsearchParseException("field must be either [{}], [{}] or [{}]", LATITUDE, LONGITUDE, GEOHASH);
                    }
                } else {
                    throw new ElasticsearchParseException("token [{}] not allowed", parser.currentToken());
                }
            }

            if (geohash != null) {
                if(!Double.isNaN(lat) || !Double.isNaN(lon)) {
                    throw new ElasticsearchParseException("field must be either lat/lon or geohash");
                } else {
                    return point.resetFromGeoHash(geohash);
                }
            } else if (numberFormatException != null) {
                throw new ElasticsearchParseException("[{}] and [{}] must be valid double values", numberFormatException, LATITUDE,
                    LONGITUDE);
            } else if (Double.isNaN(lat)) {
                throw new ElasticsearchParseException("field [{}] missing", LATITUDE);
            } else if (Double.isNaN(lon)) {
                throw new ElasticsearchParseException("field [{}] missing", LONGITUDE);
            } else {
                return point.reset(lat, lon);
            }

        } else if(parser.currentToken() == Token.START_ARRAY) {
            int element = 0;
            while(parser.nextToken() != Token.END_ARRAY) {
                if(parser.currentToken() == Token.VALUE_NUMBER) {
                    element++;
                    if(element == 1) {
                        lon = parser.doubleValue();
                    } else if(element == 2) {
                        lat = parser.doubleValue();
                    } else {
                        throw new ElasticsearchParseException("only two values allowed");
                    }
                } else {
                    throw new ElasticsearchParseException("numeric value expected");
                }
            }
            return point.reset(lat, lon);
        } else if(parser.currentToken() == Token.VALUE_STRING) {
            String data = parser.text();
            return parseGeoPoint(data, point);
        } else {
            throw new ElasticsearchParseException("geo_point expected");
        }
    }

    /** parse a {@link GeoPoint} from a String */
    public static GeoPoint parseGeoPoint(String data, GeoPoint point) {
        int comma = data.indexOf(',');
        if(comma > 0) {
            double lat = Double.parseDouble(data.substring(0, comma).trim());
            double lon = Double.parseDouble(data.substring(comma + 1).trim());
            return point.reset(lat, lon);
        } else {
            return point.resetFromGeoHash(data);
        }
    }

    /** Returns the maximum distance/radius (in meters) from the point 'center' before overlapping */
    public static double maxRadialDistanceMeters(final double centerLat, final double centerLon) {
      if (Math.abs(centerLat) == MAX_LAT) {
        return SloppyMath.haversinMeters(centerLat, centerLon, 0, centerLon);
      }
      return SloppyMath.haversinMeters(centerLat, centerLon, centerLat, (MAX_LON + centerLon) % 360);
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
        double x = (lon2 - lon1) * SloppyMath.TO_RADIANS * Math.cos((lat2 + lat1) / 2.0 * SloppyMath.TO_RADIANS);
        double y = (lat2 - lat1) * SloppyMath.TO_RADIANS;
        return Math.sqrt(x * x + y * y) * EARTH_MEAN_RADIUS;
    }

    /** check if point is within a rectangle
     * todo: move this to lucene Rectangle class
     */
    public static boolean rectangleContainsPoint(Rectangle r, double lat, double lon) {
        if (lat >= r.minLat && lat <= r.maxLat) {
            // if rectangle crosses the dateline we only check if the lon is >= min or max
            return r.crossesDateline() ? lon >= r.minLon || lon <= r.maxLon : lon >= r.minLon && lon <= r.maxLon;
        }
        return false;
    }

    /**
     * Return a {@link SortedNumericDoubleValues} instance that returns the distances to a list of geo-points
     * for each document.
     */
    public static SortedNumericDoubleValues distanceValues(final GeoDistance distance,
                                                           final DistanceUnit unit,
                                                           final MultiGeoPointValues geoPointValues,
                                                           final GeoPoint... fromPoints) {
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
                    final GeoPoint to = singleValues.geoPointValue();
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

    private GeoUtils() {
    }
}
