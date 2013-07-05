/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.QuadPrefixTree;
import org.elasticsearch.common.unit.DistanceUnit;

/**
 */
public class GeoUtils {

    /** Earth ellipsoid major axis defined by WGS 84 in meters */
    public static final double EARTH_SEMI_MAJOR_AXIS = 6378137.0;      // meters (WGS 84)

    /** Earth ellipsoid minor axis defined by WGS 84 in meters */
    public static final double EARTH_SEMI_MINOR_AXIS = 6356752.314245; // meters (WGS 84)
    
    /** Earth ellipsoid equator length in meters */
    public static final double EARTH_EQUATOR = 2*Math.PI * EARTH_SEMI_MAJOR_AXIS;

    /** Earth ellipsoid polar distance in meters */
    public static final double EARTH_POLAR_DISTANCE = Math.PI * EARTH_SEMI_MINOR_AXIS;
    
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
            return (part<=(1l<<level)) ?level :(level+1); // adjust level
        }
    }

    /**
     * Calculate the number of levels needed for a specific precision. QuadTree
     * cells will not exceed the specified size (diagonal) of the precision.
     * @param distance Maximum size of cells as unit string (must greater or equal to zero)
     * @return levels need to achieve precision  
     */
    public static int quadTreeLevelsForPrecision(String distance) {
        return quadTreeLevelsForPrecision(DistanceUnit.parse(distance, DistanceUnit.METERS, DistanceUnit.METERS));
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
        return geoHashLevelsForPrecision(DistanceUnit.parse(distance, DistanceUnit.METERS, DistanceUnit.METERS));
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
     * <p/>
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
     * <p/>
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
     * <p/>
     * You can control which coordinate gets normalized with the two flags.
     * <p/>
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
        double lat = point.lat();
        double lon = point.lon();
        
        normLat = normLat && (lat>90 || lat <= -90);
        normLon = normLon && (lon>180 || lon <= -180);
        
        if (normLat) {
            lat = centeredModulus(lat, 360);
            boolean shift = true;
            if (lat < -90) {
                lat = -180 - lat;
            } else if (lat > 90) {
                lat = 180 - lat;
            } else {
                // No need to shift the longitude, and the latitude is normalized
                shift = false;
            }
            if (shift) {
                if (normLon) {
                    lon += 180;
                } else {
                    // Longitude won't be normalized,
                    // keep it in the form x+k*360 (with x in ]-180;180])
                    // by only changing x, assuming k is meaningful for the user application.
                    lon += normalizeLon(lon) > 0 ? -180 : 180;
                }
            }
        }
        if (normLon) {
            lon = centeredModulus(lon, 360);
        }
        point.reset(lat, lon);
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

}
