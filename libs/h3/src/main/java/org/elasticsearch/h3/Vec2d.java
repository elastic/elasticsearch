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
 * Copyright 2016-2017 Uber Technologies, Inc.
 */
package org.elasticsearch.h3;

import java.util.Objects;

/**
 *  2D floating-point vector
 */
record Vec2d(
    double x, // x component
    double y  // y component
) {

    /** 1/sin(60') **/
    private static final double M_RSIN60 = 1.0 / Constants.M_SQRT3_2;

    /** one third **/
    private static final double M_ONETHIRD = 1.0 / 3.0;

    private static final double VEC2D_RESOLUTION = 1e-7;

    /**
     * icosahedron face centers in lat/lng radians
     */
    public static final LatLng[] faceCenterGeo = new LatLng[] {
        new LatLng(0.803582649718989942, 1.248397419617396099),    // face 0
        new LatLng(1.307747883455638156, 2.536945009877921159),    // face 1
        new LatLng(1.054751253523952054, -1.347517358900396623),   // face 2
        new LatLng(0.600191595538186799, -0.450603909469755746),   // face 3
        new LatLng(0.491715428198773866, 0.401988202911306943),    // face 4
        new LatLng(0.172745327415618701, 1.678146885280433686),    // face 5
        new LatLng(0.605929321571350690, 2.953923329812411617),    // face 6
        new LatLng(0.427370518328979641, -1.888876200336285401),   // face 7
        new LatLng(-0.079066118549212831, -0.733429513380867741),  // face 8
        new LatLng(-0.230961644455383637, 0.506495587332349035),   // face 9
        new LatLng(0.079066118549212831, 2.408163140208925497),    // face 10
        new LatLng(0.230961644455383637, -2.635097066257444203),   // face 11
        new LatLng(-0.172745327415618701, -1.463445768309359553),  // face 12
        new LatLng(-0.605929321571350690, -0.187669323777381622),  // face 13
        new LatLng(-0.427370518328979641, 1.252716453253507838),   // face 14
        new LatLng(-0.600191595538186799, 2.690988744120037492),   // face 15
        new LatLng(-0.491715428198773866, -2.739604450678486295),  // face 16
        new LatLng(-0.803582649718989942, -1.893195233972397139),  // face 17
        new LatLng(-1.307747883455638156, -0.604647643711872080),  // face 18
        new LatLng(-1.054751253523952054, 1.794075294689396615),   // face 19
    };

    /**
     * icosahedron face ijk axes as azimuth in radians from face center to
     * vertex 0/1/2 respectively
     */
    public static final double[][] faceAxesAzRadsCII = new double[][] {
        { 5.619958268523939882, 3.525563166130744542, 1.431168063737548730 },  // face 0
        { 5.760339081714187279, 3.665943979320991689, 1.571548876927796127 },  // face 1
        { 0.780213654393430055, 4.969003859179821079, 2.874608756786625655 },  // face 2
        { 0.430469363979999913, 4.619259568766391033, 2.524864466373195467 },  // face 3
        { 6.130269123335111400, 4.035874020941915804, 1.941478918548720291 },  // face 4
        { 2.692877706530642877, 0.598482604137447119, 4.787272808923838195 },  // face 5
        { 2.982963003477243874, 0.888567901084048369, 5.077358105870439581 },  // face 6
        { 3.532912002790141181, 1.438516900396945656, 5.627307105183336758 },  // face 7
        { 3.494305004259568154, 1.399909901866372864, 5.588700106652763840 },  // face 8
        { 3.003214169499538391, 0.908819067106342928, 5.097609271892733906 },  // face 9
        { 5.930472956509811562, 3.836077854116615875, 1.741682751723420374 },  // face 10
        { 0.138378484090254847, 4.327168688876645809, 2.232773586483450311 },  // face 11
        { 0.448714947059150361, 4.637505151845541521, 2.543110049452346120 },  // face 12
        { 0.158629650112549365, 4.347419854898940135, 2.253024752505744869 },  // face 13
        { 5.891865957979238535, 3.797470855586042958, 1.703075753192847583 },  // face 14
        { 2.711123289609793325, 0.616728187216597771, 4.805518392002988683 },  // face 15
        { 3.294508837434268316, 1.200113735041072948, 5.388903939827463911 },  // face 16
        { 3.804819692245439833, 1.710424589852244509, 5.899214794638635174 },  // face 17
        { 3.664438879055192436, 1.570043776661997111, 5.758833981448388027 },  // face 18
        { 2.361378999196363184, 0.266983896803167583, 4.455774101589558636 },  // face 19
    };

    /**
     * Determines the center point in spherical coordinates of a cell given by this 2D
     * hex coordinates on a particular icosahedral face.
     *
     * @param face      The icosahedral face upon which the 2D hex coordinate system is
     *                  centered.
     * @param res       The H3 resolution of the cell.
     * @param substrate Indicates whether or not this grid is actually a substrate
     *                  grid relative to the specified resolution.
     */
    public LatLng hex2dToGeo(int face, int res, boolean substrate) {
        return hex2dToGeo(this.x, this.y, face, res, substrate);
    }

    /**
     * Determines the center point in spherical coordinates of a cell given by the provided 2D
     * hex coordinates on a particular icosahedral face.
     *
     * @param x         The x component of the 2D hex coordinates.
     * @param y         The y component of the 2D hex coordinates.
     * @param face      The icosahedral face upon which the 2D hex coordinate system is
     *                  centered.
     * @param res       The H3 resolution of the cell.
     * @param substrate Indicates whether or not this grid is actually a substrate
     *                  grid relative to the specified resolution.
     */
    static LatLng hex2dToGeo(double x, double y, int face, int res, boolean substrate) {
        // calculate (r, theta) in hex2d
        double r = Math.sqrt(x * x + y * y);

        if (r < Constants.EPSILON) {
            return faceCenterGeo[face];
        }

        double theta = FastMath.atan2(y, x);

        // scale for current resolution length u
        for (int i = 0; i < res; i++) {
            r *= Constants.M_RSQRT7;
        }

        // scale accordingly if this is a substrate grid
        if (substrate) {
            r *= M_ONETHIRD;
            if (H3Index.isResolutionClassIII(res)) {
                r *= Constants.M_RSQRT7;
            }
        }

        r *= Constants.RES0_U_GNOMONIC;

        // perform inverse gnomonic scaling of r
        r = FastMath.atan(r);

        // adjust theta for Class III
        // if a substrate grid, then it's already been adjusted for Class III
        if (substrate == false && H3Index.isResolutionClassIII(res)) {
            theta = posAngleRads(theta + Constants.M_AP7_ROT_RADS);
        }

        // find theta as an azimuth
        theta = posAngleRads(faceAxesAzRadsCII[face][0] - theta);
        // now find the point at (r,theta) from the face center
        return Vec3d.faceCenterPoint[face].geoAzDistanceRads(theta, r);
    }

    /**
     * Determine the containing hex in ijk+ coordinates for a 2D cartesian
     * coordinate vector (from DGGRID).
     *
     */
    static CoordIJK hex2dToCoordIJK(double x, double y) {
        final double a1, a2;
        final double x1, x2;
        final int m1, m2;
        final double r1, r2;

        // quantize into the ij system and then normalize
        final int k = 0;
        int i;
        int j;

        a1 = Math.abs(x);
        a2 = Math.abs(y);

        // first do a reverse conversion
        x2 = a2 * M_RSIN60;
        x1 = a1 + x2 * 0.5;

        // check if we have the center of a hex
        m1 = (int) x1;
        m2 = (int) x2;

        // otherwise round correctly
        r1 = x1 - m1;
        r2 = x2 - m2;

        if (r1 < 0.5) {
            if (r1 < M_ONETHIRD) {
                if (r2 < (1.0 + r1) * 0.5) {
                    i = m1;
                    j = m2;
                } else {
                    i = m1;
                    j = m2 + 1;
                }
            } else {
                if (r2 < (1.0 - r1)) {
                    j = m2;
                } else {
                    j = m2 + 1;
                }

                if ((1.0 - r1) <= r2 && r2 < (2.0 * r1)) {
                    i = m1 + 1;
                } else {
                    i = m1;
                }
            }
        } else {
            if (r1 < 2.0 * M_ONETHIRD) {
                if (r2 < (1.0 - r1)) {
                    j = m2;
                } else {
                    j = m2 + 1;
                }

                if ((2.0 * r1 - 1.0) < r2 && r2 < (1.0 - r1)) {
                    i = m1;
                } else {
                    i = m1 + 1;
                }
            } else {
                if (r2 < (r1 * 0.5)) {
                    i = m1 + 1;
                    j = m2;
                } else {
                    i = m1 + 1;
                    j = m2 + 1;
                }
            }
        }

        // now fold across the axes if necessary

        if (x < 0.0) {
            if ((j % 2) == 0)  // even
            {
                final int axisi = j / 2;
                final int diff = i - axisi;
                i = i - (2 * diff);
            } else {
                final int axisi = (j + 1) / 2;
                final int diff = i - axisi;
                i = i - ((2 * diff) + 1);
            }
        }

        if (y < 0.0) {

            i = i - ((2 * j + 1) / 2);
            j *= -1;
        }
        final CoordIJK coordIJK = new CoordIJK(i, j, k);
        coordIJK.ijkNormalize();
        return coordIJK;
    }

    public boolean numericallyIdentical(Vec2d vec2d) {
        return Math.abs(vec2d.x - x) < VEC2D_RESOLUTION && Math.abs(vec2d.y - y) < VEC2D_RESOLUTION;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Vec2d vec2d = (Vec2d) o;
        return Double.compare(vec2d.x, x) == 0 && Double.compare(vec2d.y, y) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(x, y);
    }

    /**
     * Finds the intersection between two lines. Assumes that the lines intersect
     * and that the intersection is not at an endpoint of either line.
     *
     * @param p0 The first endpoint of the first line.
     * @param p1 The second endpoint of the first line.
     * @param p2 The first endpoint of the second line.
     * @param p3 The second endpoint of the second line.
     */
    public static Vec2d v2dIntersect(Vec2d p0, Vec2d p1, Vec2d p2, Vec2d p3) {
        final double s1x = p1.x - p0.x;
        final double s1y = p1.y - p0.y;
        final double s2x = p3.x - p2.x;
        final double s2y = p3.y - p2.y;

        final double t = ((s2x * (p0.y - p2.y) - s2y * (p0.x - p2.x)) / (-s2x * s1y + s1x * s2y));

        return new Vec2d(p0.x + (t * s1x), p0.y + (t * s1y));
    }

    /**
     * Normalizes radians to a value between 0.0 and two PI.
     *
     * @param rads The input radians value.
     * @return The normalized radians value.
     */
    static double posAngleRads(double rads) {
        if (rads < 0.0) {
            return rads + Constants.M_2PI;
        } else if (rads >= Constants.M_2PI) {
            return rads - Constants.M_2PI;
        } else {
            return rads;
        }
    }
}
