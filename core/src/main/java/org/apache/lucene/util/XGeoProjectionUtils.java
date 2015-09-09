package org.apache.lucene.util;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Reusable geo-spatial projection utility methods.
 *
 * @lucene.experimental
 */
public class XGeoProjectionUtils {
    // WGS84 earth-ellipsoid major (a) minor (b) radius, (f) flattening and eccentricity (e)
    static final double SEMIMAJOR_AXIS = 6_378_137; // [m]
    static final double FLATTENING = 1.0/298.257223563;
    static final double SEMIMINOR_AXIS = SEMIMAJOR_AXIS * (1.0 - FLATTENING); //6_356_752.31420; // [m]
    static final double ECCENTRICITY = StrictMath.sqrt((2.0 - FLATTENING) * FLATTENING);
    static final double PI_OVER_2 = StrictMath.PI / 2.0D;
    static final double SEMIMAJOR_AXIS2 = SEMIMAJOR_AXIS * SEMIMAJOR_AXIS;
    static final double SEMIMINOR_AXIS2 = SEMIMINOR_AXIS * SEMIMINOR_AXIS;

    /**
     * Converts from geocentric earth-centered earth-fixed to geodesic lat/lon/alt
     * @param x Cartesian x coordinate
     * @param y Cartesian y coordinate
     * @param z Cartesian z coordinate
     * @param lla 0: longitude 1: latitude: 2: altitude
     * @return double array as 0: longitude 1: latitude 2: altitude
     */
    public static final double[] ecfToLLA(final double x, final double y, final double z, double[] lla) {
        boolean atPole = false;
        final double ad_c = 1.0026000D;
        final double e2 = (SEMIMAJOR_AXIS2 - SEMIMINOR_AXIS2)/(SEMIMAJOR_AXIS2);
        final double ep2 = (SEMIMAJOR_AXIS2 - SEMIMINOR_AXIS2)/(SEMIMINOR_AXIS2);
        final double cos67P5 = 0.38268343236508977D;

        if (lla == null) {
            lla = new double[3];
        }

        if (x != 0.0) {
            lla[0] = StrictMath.atan2(y,x);
        } else {
            if (y > 0) {
                lla[0] = PI_OVER_2;
            } else if (y < 0) {
                lla[0] = -PI_OVER_2;
            } else {
                atPole = true;
                lla[0] = 0.0D;
                if (z > 0.0) {
                    lla[1] = PI_OVER_2;
                } else if (z < 0.0) {
                    lla[1] = -PI_OVER_2;
                } else {
                    lla[1] = PI_OVER_2;
                    lla[2] = -SEMIMINOR_AXIS;
                    return lla;
                }
            }
        }

        final double w2 = x*x + y*y;
        final double w = StrictMath.sqrt(w2);
        final double t0 = z * ad_c;
        final double s0 = StrictMath.sqrt(t0 * t0 + w2);
        final double sinB0 = t0 / s0;
        final double cosB0 = w / s0;
        final double sin3B0 = sinB0 * sinB0 * sinB0;
        final double t1 = z + SEMIMINOR_AXIS * ep2 * sin3B0;
        final double sum = w - SEMIMAJOR_AXIS * e2 * cosB0 * cosB0 * cosB0;
        final double s1 = StrictMath.sqrt(t1 * t1 + sum * sum);
        final double sinP1 = t1 / s1;
        final double cosP1 = sum / s1;
        final double rn = SEMIMAJOR_AXIS / StrictMath.sqrt(1.0D - e2 * sinP1 * sinP1);

        if (cosP1 >= cos67P5) {
            lla[2] = w / cosP1 - rn;
        } else if (cosP1 <= -cos67P5) {
            lla[2] = w / -cosP1 - rn;
        } else {
            lla[2] = z / sinP1 + rn * (e2 - 1.0);
        }
        if (!atPole) {
            lla[1] = StrictMath.atan(sinP1/cosP1);
        }
        lla[0] = StrictMath.toDegrees(lla[0]);
        lla[1] = StrictMath.toDegrees(lla[1]);

        return lla;
    }

    /**
     * Converts from geodesic lon lat alt to geocentric earth-centered earth-fixed
     * @param lon geodesic longitude
     * @param lat geodesic latitude
     * @param alt geodesic altitude
     * @param ecf reusable earth-centered earth-fixed result
     * @return either a new ecef array or the reusable ecf parameter
     */
    public static final double[] llaToECF(double lon, double lat, double alt, double[] ecf) {
        lon = StrictMath.toRadians(lon);
        lat = StrictMath.toRadians(lat);

        final double sl = StrictMath.sin(lat);
        final double s2 = sl*sl;
        final double cl = StrictMath.cos(lat);
        final double ge2 = (SEMIMAJOR_AXIS2 - SEMIMINOR_AXIS2)/(SEMIMAJOR_AXIS2);

        if (ecf == null) {
            ecf = new double[3];
        }

        if (lat < -PI_OVER_2 && lat > -1.001D * PI_OVER_2) {
            lat = -PI_OVER_2;
        } else if (lat > PI_OVER_2 && lat < 1.001D * PI_OVER_2) {
            lat = PI_OVER_2;
        }
        assert (lat >= -PI_OVER_2) || (lat <= PI_OVER_2);

        if (lon > StrictMath.PI) {
            lon -= (2*StrictMath.PI);
        }

        final double rn = SEMIMAJOR_AXIS / StrictMath.sqrt(1.0D - ge2 * s2);
        ecf[0] = (rn+alt) * cl * StrictMath.cos(lon);
        ecf[1] = (rn+alt) * cl * StrictMath.sin(lon);
        ecf[2] = ((rn*(1.0-ge2))+alt)*sl;

        return ecf;
    }

    /**
     * Converts from lat lon alt (in degrees) to East North Up right-hand coordinate system
     * @param lon longitude in degrees
     * @param lat latitude in degrees
     * @param alt altitude in meters
     * @param centerLon reference point longitude in degrees
     * @param centerLat reference point latitude in degrees
     * @param centerAlt reference point altitude in meters
     * @param enu result east, north, up coordinate
     * @return east, north, up coordinate
     */
    public static double[] llaToENU(final double lon, final double lat, final double alt, double centerLon,
                                    double centerLat, final double centerAlt, double[] enu) {
        if (enu == null) {
            enu = new double[3];
        }

        // convert point to ecf coordinates
        final double[] ecf = llaToECF(lon, lat, alt, null);

        // convert from ecf to enu
        return ecfToENU(ecf[0], ecf[1], ecf[2], centerLon, centerLat, centerAlt, enu);
    }

    /**
     * Converts from East North Up right-hand rule to lat lon alt in degrees
     * @param x easting (in meters)
     * @param y northing (in meters)
     * @param z up (in meters)
     * @param centerLon reference point longitude (in degrees)
     * @param centerLat reference point latitude (in degrees)
     * @param centerAlt reference point altitude (in meters)
     * @param lla resulting lat, lon, alt point (in degrees)
     * @return lat, lon, alt point (in degrees)
     */
    public static double[] enuToLLA(final double x, final double y, final double z, final double centerLon,
                                    final double centerLat, final double centerAlt, double[] lla) {
        // convert enuToECF
        if (lla == null) {
            lla = new double[3];
        }

        // convert enuToECF, storing intermediate result in lla
        lla = enuToECF(x, y, z, centerLon, centerLat, centerAlt, lla);

        // convert ecf to LLA
        return ecfToLLA(lla[0], lla[1], lla[2], lla);
    }

    /**
     * Convert from Earth-Centered-Fixed to Easting, Northing, Up Right Hand System
     * @param x ECF X coordinate (in meters)
     * @param y ECF Y coordinate (in meters)
     * @param z ECF Z coordinate (in meters)
     * @param centerLon ENU origin longitude (in degrees)
     * @param centerLat ENU origin latitude (in degrees)
     * @param centerAlt ENU altitude (in meters)
     * @param enu reusable enu result
     * @return Easting, Northing, Up coordinate
     */
    public static double[] ecfToENU(double x, double y, double z, final double centerLon,
                                    final double centerLat, final double centerAlt, double[] enu) {
        if (enu == null) {
            enu = new double[3];
        }

        // create rotation matrix and rotate to enu orientation
        final double[][] phi = createPhiTransform(centerLon, centerLat, null);

        // convert origin to ENU
        final double[] originECF = llaToECF(centerLon, centerLat, centerAlt, null);
        final double[] originENU = new double[3];
        originENU[0] = ((phi[0][0] * originECF[0]) + (phi[0][1] * originECF[1]) + (phi[0][2] * originECF[2]));
        originENU[1] = ((phi[1][0] * originECF[0]) + (phi[1][1] * originECF[1]) + (phi[1][2] * originECF[2]));
        originENU[2] = ((phi[2][0] * originECF[0]) + (phi[2][1] * originECF[1]) + (phi[2][2] * originECF[2]));

        // rotate then translate
        enu[0] = ((phi[0][0] * x) + (phi[0][1] * y) + (phi[0][2] * z)) - originENU[0];
        enu[1] = ((phi[1][0] * x) + (phi[1][1] * y) + (phi[1][2] * z)) - originENU[1];
        enu[2] = ((phi[2][0] * x) + (phi[2][1] * y) + (phi[2][2] * z)) - originENU[2];

        return enu;
    }

    /**
     * Convert from Easting, Northing, Up Right-Handed system to Earth Centered Fixed system
     * @param x ENU x coordinate (in meters)
     * @param y ENU y coordinate (in meters)
     * @param z ENU z coordinate (in meters)
     * @param centerLon ENU origin longitude (in degrees)
     * @param centerLat ENU origin latitude (in degrees)
     * @param centerAlt ENU origin altitude (in meters)
     * @param ecf reusable ecf result
     * @return ecf result coordinate
     */
    public static double[] enuToECF(final double x, final double y, final double z, double centerLon,
                                    double centerLat, final double centerAlt, double[] ecf) {
        if (ecf == null) {
            ecf = new double[3];
        }

        double[][] phi = createTransposedPhiTransform(centerLon, centerLat, null);
        double[] ecfOrigin = llaToECF(centerLon, centerLat, centerAlt, null);

        // rotate and translate
        ecf[0] = (phi[0][0]*x + phi[0][1]*y + phi[0][2]*z) + ecfOrigin[0];
        ecf[1] = (phi[1][0]*x + phi[1][1]*y + phi[1][2]*z) + ecfOrigin[1];
        ecf[2] = (phi[2][0]*x + phi[2][1]*y + phi[2][2]*z) + ecfOrigin[2];

        return ecf;
    }

    /**
     * Create the rotation matrix for converting Earth Centered Fixed to Easting Northing Up
     * @param originLon ENU origin longitude (in degrees)
     * @param originLat ENU origin latitude (in degrees)
     * @param phiMatrix reusable phi matrix result
     * @return phi rotation matrix
     */
    private static double[][] createPhiTransform(double originLon, double originLat, double[][] phiMatrix) {

        if (phiMatrix == null) {
            phiMatrix = new double[3][3];
        }

        originLon = StrictMath.toRadians(originLon);
        originLat = StrictMath.toRadians(originLat);

        final double sLon = StrictMath.sin(originLon);
        final double cLon = StrictMath.cos(originLon);
        final double sLat = StrictMath.sin(originLat);
        final double cLat = StrictMath.cos(originLat);

        phiMatrix[0][0] = -sLon;
        phiMatrix[0][1] = cLon;
        phiMatrix[0][2] = 0.0D;
        phiMatrix[1][0] = -sLat * cLon;
        phiMatrix[1][1] = -sLat * sLon;
        phiMatrix[1][2] = cLat;
        phiMatrix[2][0] = cLat * cLon;
        phiMatrix[2][1] = cLat * sLon;
        phiMatrix[2][2] = sLat;

        return phiMatrix;
    }

    /**
     * Create the transposed rotation matrix for converting Easting Northing Up coordinates to Earth Centered Fixed
     * @param originLon ENU origin longitude (in degrees)
     * @param originLat ENU origin latitude (in degrees)
     * @param phiMatrix reusable phi rotation matrix result
     * @return transposed phi rotation matrix
     */
    private static double[][] createTransposedPhiTransform(double originLon, double originLat, double[][] phiMatrix) {

        if (phiMatrix == null) {
            phiMatrix = new double[3][3];
        }

        originLon = StrictMath.toRadians(originLon);
        originLat = StrictMath.toRadians(originLat);

        final double sLat = StrictMath.sin(originLat);
        final double cLat = StrictMath.cos(originLat);
        final double sLon = StrictMath.sin(originLon);
        final double cLon = StrictMath.cos(originLon);

        phiMatrix[0][0] = -sLon;
        phiMatrix[1][0] = cLon;
        phiMatrix[2][0] = 0.0D;
        phiMatrix[0][1] = -sLat * cLon;
        phiMatrix[1][1] = -sLat * sLon;
        phiMatrix[2][1] = cLat;
        phiMatrix[0][2] = cLat * cLon;
        phiMatrix[1][2] = cLat * sLon;
        phiMatrix[2][2] = sLat;

        return phiMatrix;
    }

    /**
     * Finds a point along a bearing from a given lon,lat geolocation using vincenty's distance formula
     *
     * @param lon origin longitude in degrees
     * @param lat origin latitude in degrees
     * @param bearing azimuthal bearing in degrees
     * @param dist distance in meters
     * @param pt resulting point
     * @return the point along a bearing at a given distance in meters
     */
    public static final double[] pointFromLonLatBearing(double lon, double lat, double bearing, double dist, double[] pt) {

        if (pt == null) {
            pt = new double[2];
        }

        final double alpha1 = StrictMath.toRadians(bearing);
        final double cosA1 = StrictMath.cos(alpha1);
        final double sinA1 = StrictMath.sin(alpha1);
        final double tanU1 = (1-FLATTENING) * StrictMath.tan(StrictMath.toRadians(lat));
        final double cosU1 = 1 / StrictMath.sqrt((1+tanU1*tanU1));
        final double sinU1 = tanU1*cosU1;
        final double sig1 = StrictMath.atan2(tanU1, cosA1);
        final double sinAlpha = cosU1 * sinA1;
        final double cosSqAlpha = 1 - sinAlpha*sinAlpha;
        final double uSq = cosSqAlpha * (SEMIMAJOR_AXIS2 - SEMIMINOR_AXIS2) / SEMIMINOR_AXIS2;
        final double A = 1 + uSq/16384D*(4096D + uSq * (-768D + uSq * (320D - 175D*uSq)));
        final double B = uSq/1024D * (256D + uSq * (-128D + uSq * (74D - 47D * uSq)));

        double sigma = dist / (SEMIMINOR_AXIS*A);
        double sigmaP;
        double sinSigma, cosSigma, cos2SigmaM, deltaSigma;

        do {
            cos2SigmaM = StrictMath.cos(2*sig1 + sigma);
            sinSigma = StrictMath.sin(sigma);
            cosSigma = StrictMath.cos(sigma);

            deltaSigma = B * sinSigma * (cos2SigmaM + (B/4D) * (cosSigma*(-1+2*cos2SigmaM*cos2SigmaM)-
                    (B/6) * cos2SigmaM*(-3+4*sinSigma*sinSigma)*(-3+4*cos2SigmaM*cos2SigmaM)));
            sigmaP = sigma;
            sigma = dist / (SEMIMINOR_AXIS*A) + deltaSigma;
        } while (StrictMath.abs(sigma-sigmaP) > 1E-12);

        final double tmp = sinU1*sinSigma - cosU1*cosSigma*cosA1;
        final double lat2 = StrictMath.atan2(sinU1*cosSigma + cosU1*sinSigma*cosA1,
                (1-FLATTENING) * StrictMath.sqrt(sinAlpha*sinAlpha + tmp*tmp));
        final double lambda = StrictMath.atan2(sinSigma*sinA1, cosU1*cosSigma - sinU1*sinSigma*cosA1);
        final double c = FLATTENING/16 * cosSqAlpha * (4 + FLATTENING * (4 - 3 * cosSqAlpha));

        final double lam = lambda - (1-c) * FLATTENING * sinAlpha *
                (sigma + c * sinSigma * (cos2SigmaM + c * cosSigma * (-1 + 2* cos2SigmaM*cos2SigmaM)));
        pt[0] = lon + StrictMath.toDegrees(lam);
        pt[1] = StrictMath.toDegrees(lat2);

        return pt;
    }
}