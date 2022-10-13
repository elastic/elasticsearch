/*
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * This project is based on a modification of https://github.com/uber/h3 which is licensed under the Apache 2.0 License.
 *
 * Copyright 2016-2021 Uber Technologies, Inc.
 */
package org.elasticsearch.h3;

/** pair of latitude/longitude */
public final class LatLng {

    // lat / lon in radians
    private final double lon;
    private final double lat;

    LatLng(double lat, double lon) {
        this.lon = lon;
        this.lat = lat;
    }

    /** Returns latitude in radians */
    public double getLatRad() {
        return lat;
    }

    /** Returns longitude in radians */
    public double getLonRad() {
        return lon;
    }

    /** Returns latitude in degrees */
    public double getLatDeg() {
        return Math.toDegrees(getLatRad());
    }

    /** Returns longitude in degrees */
    public double getLonDeg() {
        return Math.toDegrees(getLonRad());
    }

    /**
     * Encodes a coordinate on the sphere to the corresponding icosahedral face and
     * containing 2D hex coordinates relative to that face center.
     *
     * @param res The desired H3 resolution for the encoding.
     */
    FaceIJK geoToFaceIJK(int res) {
        Vec3d v3d = new Vec3d(this);

        // determine the icosahedron face
        int face = 0;
        double sqd = v3d.pointSquareDist(Vec3d.faceCenterPoint[0]);
        for (int i = 1; i < Vec3d.faceCenterPoint.length; i++) {
            double sqdT = v3d.pointSquareDist(Vec3d.faceCenterPoint[i]);
            if (sqdT < sqd) {
                face = i;
                sqd = sqdT;
            }
        }
        // cos(r) = 1 - 2 * sin^2(r/2) = 1 - 2 * (sqd / 4) = 1 - sqd/2
        double r = Math.acos(1 - sqd / 2);

        if (r < Constants.EPSILON) {
            return new FaceIJK(face, new Vec2d(0.0, 0.0).hex2dToCoordIJK());
        }

        // now have face and r, now find CCW theta from CII i-axis
        double theta = Vec2d.posAngleRads(
            Vec2d.faceAxesAzRadsCII[face][0] - Vec2d.posAngleRads(Vec2d.faceCenterGeo[face].geoAzimuthRads(this))
        );

        // adjust theta for Class III (odd resolutions)
        if (H3Index.isResolutionClassIII(res)) {
            theta = Vec2d.posAngleRads(theta - Constants.M_AP7_ROT_RADS);
        }

        // perform gnomonic scaling of r
        r = Math.tan(r);

        // scale for current resolution length u
        r /= Constants.RES0_U_GNOMONIC;
        for (int i = 0; i < res; i++) {
            r *= Constants.M_SQRT7;
        }

        // we now have (r, theta) in hex2d with theta ccw from x-axes

        // convert to local x,y
        Vec2d vec2d = new Vec2d(r * Math.cos(theta), r * Math.sin(theta));
        return new FaceIJK(face, vec2d.hex2dToCoordIJK());
    }

    /**
     * Determines the azimuth to the provided LatLng in radians.
     *
     * @param p The spherical coordinates.
     * @return The azimuth in radians.
     */
    private double geoAzimuthRads(LatLng p) {
        return Math.atan2(
            Math.cos(p.lat) * Math.sin(p.lon - lon),
            Math.cos(lat) * Math.sin(p.lat) - Math.sin(lat) * Math.cos(p.lat) * Math.cos(p.lon - lon)
        );
    }
}
