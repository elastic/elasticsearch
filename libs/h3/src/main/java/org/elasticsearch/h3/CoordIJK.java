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
 * Copyright 2016-2018, 2020-2021 Uber Technologies, Inc.
 */
package org.elasticsearch.h3;

/**
 * Mutable IJK hexagon coordinates
 *
 * Each axis is spaced 120 degrees apart.
 *
 * References two Vec2d cartesian coordinate systems:
 *
 *    1. gnomonic: face-centered polyhedral gnomonic projection space with
 *             traditional scaling and x-axes aligned with the face Class II
 *             i-axes.
 *
 *    2. hex2d: local face-centered coordinate system scaled a specific H3 grid
 *             resolution unit length and with x-axes aligned with the local
 *             i-axes
 */
final class CoordIJK {

    /** one seventh (1/7) **/
    private static final double M_ONESEVENTH = 1.0 / 7.0;

    /** CoordIJK unit vectors corresponding to the 7 H3 digits.
     */
    private static final int[][] UNIT_VECS = {
        { 0, 0, 0 },  // direction 0
        { 0, 0, 1 },  // direction 1
        { 0, 1, 0 },  // direction 2
        { 0, 1, 1 },  // direction 3
        { 1, 0, 0 },  // direction 4
        { 1, 0, 1 },  // direction 5
        { 1, 1, 0 }   // direction 6
    };

    /** H3 digit representing ijk+ axes direction.
     * Values will be within the lowest 3 bits of an integer.
     */
    public enum Direction {

        CENTER_DIGIT(0),
        K_AXES_DIGIT(1),
        J_AXES_DIGIT(2),
        JK_AXES_DIGIT(J_AXES_DIGIT.digit() | K_AXES_DIGIT.digit()),
        I_AXES_DIGIT(4),
        IK_AXES_DIGIT(I_AXES_DIGIT.digit() | K_AXES_DIGIT.digit()),
        IJ_AXES_DIGIT(I_AXES_DIGIT.digit() | J_AXES_DIGIT.digit()),
        INVALID_DIGIT(7),
        NUM_DIGITS(INVALID_DIGIT.digit()),
        PENTAGON_SKIPPED_DIGIT(K_AXES_DIGIT.digit());

        Direction(int digit) {
            this.digit = digit;
        }

        private final int digit;

        public int digit() {
            return digit;
        }

    }

    int i;  // i component
    int j;  // j component
    int k;  // k component

    CoordIJK(int i, int j, int k) {
        this.i = i;
        this.j = j;
        this.k = k;
    }

    /**
     * Reset the value of the IJK coordinates to the provided ones.
     *
     * @param i the i coordinate
     * @param j the j coordinate
     * @param k the k coordinate
     */
    void reset(int i, int j, int k) {
        this.i = i;
        this.j = j;
        this.k = k;
    }

    /**
     * Find the center point in 2D cartesian coordinates of a hex.
     */
    public Vec2d ijkToHex2d() {
        final int i = this.i - this.k;
        final int j = this.j - this.k;
        return new Vec2d(i - 0.5 * j, j * Constants.M_SQRT3_2);
    }

    /**
     * Find the center point in spherical coordinates of a hex on a particular icosahedral face.
     */
    public LatLng ijkToGeo(int face, int res, boolean substrate) {
        final int i = this.i - this.k;
        final int j = this.j - this.k;
        return Vec2d.hex2dToGeo(i - 0.5 * j, j * Constants.M_SQRT3_2, face, res, substrate);
    }

    /**
     * Add ijk coordinates.
     *
     * @param i the i coordinate
     * @param j the j coordinate
     * @param k the k coordinate
     */

    public void ijkAdd(int i, int j, int k) {
        this.i += i;
        this.j += j;
        this.k += k;
    }

    /**
     * Subtract ijk coordinates.
     *
     * @param i the i coordinate
     * @param j the j coordinate
     * @param k the k coordinate
     */
    public void ijkSub(int i, int j, int k) {
        this.i -= i;
        this.j -= j;
        this.k -= k;
    }

    /**
     * Normalizes ijk coordinates by setting the ijk coordinates
     * to the smallest possible positive values.
     */
    public void ijkNormalize() {
        final int min = Math.min(i, Math.min(j, k));
        ijkSub(min, min, min);
    }

    /**
     * Find the normalized ijk coordinates of the hex centered on the current
     * hex at the next finer aperture 7 counter-clockwise resolution.
     */
    public void downAp7() {
        // res r unit vectors in res r+1
        // iVec (3, 0, 1)
        // jVec (1, 3, 0)
        // kVec (0, 1, 3)
        final int i = this.i * 3 + this.j;
        final int j = this.j * 3 + this.k;
        final int k = this.k * 3 + this.i;
        this.i = i;
        this.j = j;
        this.k = k;
        ijkNormalize();
    }

    /**
     * Find the normalized ijk coordinates of the hex centered on the current
     * hex at the next finer aperture 7 clockwise resolution.
     */
    public void downAp7r() {
        // iVec (3, 1, 0)
        // jVec (0, 3, 1)
        // kVec (1, 0, 3)
        final int i = this.i * 3 + this.k;
        final int j = this.j * 3 + this.i;
        final int k = this.k * 3 + this.j;
        this.i = i;
        this.j = j;
        this.k = k;
        ijkNormalize();
    }

    /**
     * Find the normalized ijk coordinates of the hex centered on the current
     * hex at the next finer aperture 3 counter-clockwise resolution.
     */
    public void downAp3() {
        // res r unit vectors in res r+1
        // iVec (2, 0, 1)
        // jVec (1, 2, 0)
        // kVec (0, 1, 2)
        final int i = this.i * 2 + this.j;
        final int j = this.j * 2 + this.k;
        final int k = this.k * 2 + this.i;
        this.i = i;
        this.j = j;
        this.k = k;
        ijkNormalize();
    }

    /**
     * Find the normalized ijk coordinates of the hex centered on the current
     * hex at the next finer aperture 3 clockwise resolution.
     */
    public void downAp3r() {
        // res r unit vectors in res r+1
        // iVec (2, 1, 0)
        // jVec (0, 2, 1)
        // kVec (1, 0, 2)
        final int i = this.i * 2 + this.k;
        final int j = this.j * 2 + this.i;
        final int k = this.k * 2 + this.j;
        this.i = i;
        this.j = j;
        this.k = k;
        ijkNormalize();
    }

    /**
     * Rotates ijk coordinates 60 degrees clockwise.
     *
     */
    public void ijkRotate60cw() {
        // unit vector rotations
        // iVec (1, 0, 1)
        // jVec (1, 1, 0)
        // kVec (0, 1, 1)
        final int i = this.i + this.j;
        final int j = this.j + this.k;
        final int k = this.i + this.k;
        this.i = i;
        this.j = j;
        this.k = k;
        ijkNormalize();
    }

    /**
     * Rotates ijk coordinates 60 degrees counter-clockwise.
     */
    public void ijkRotate60ccw() {
        // unit vector rotations
        // iVec (1, 1, 0)
        // jVec (0, 1, 1)
        // kVec (1, 0, 1)
        final int i = this.i + this.k;
        final int j = this.i + this.j;
        final int k = this.j + this.k;
        this.i = i;
        this.j = j;
        this.k = k;
        ijkNormalize();
    }

    /**
     * Find the normalized ijk coordinates of the hex in the specified digit
     * direction from the current ijk coordinates.
     * @param digit The digit direction from the original ijk coordinates.
     */
    public void neighbor(int digit) {
        if (digit > Direction.CENTER_DIGIT.digit() && digit < Direction.NUM_DIGITS.digit()) {
            ijkAdd(UNIT_VECS[digit][0], UNIT_VECS[digit][1], UNIT_VECS[digit][2]);
            ijkNormalize();
        }
    }

    /**
     * Find the normalized ijk coordinates of the indexing parent of a cell in a
     * clockwise aperture 7 grid.
     */
    public void upAp7r() {
        final int i = this.i - this.k;
        final int j = this.j - this.k;
        this.i = (int) Math.round((2 * i + j) * M_ONESEVENTH);
        this.j = (int) Math.round((3 * j - i) * M_ONESEVENTH);
        this.k = 0;
        ijkNormalize();
    }

    /**
     * Find the normalized ijk coordinates of the indexing parent of a cell in a
     * counter-clockwise aperture 7 grid.
     *
     */
    public void upAp7() {
        final int i = this.i - this.k;
        final int j = this.j - this.k;
        this.i = (int) Math.round((3 * i - j) * M_ONESEVENTH);
        this.j = (int) Math.round((2 * j + i) * M_ONESEVENTH);
        this.k = 0;
        ijkNormalize();
    }

    /**
     * Determines the H3 digit corresponding to a unit vector in ijk coordinates.
     *
     * @return The H3 digit (0-6) corresponding to the ijk unit vector, or
     * INVALID_DIGIT on failure.
     */
    public int unitIjkToDigit() {
        // should be call on a normalized object
        if (Math.min(i, Math.min(j, k)) < 0 || Math.max(i, Math.max(j, k)) > 1) {
            return Direction.INVALID_DIGIT.digit();
        }
        return i << 2 | j << 1 | k;
    }

    /**
     * Rotates indexing digit 60 degrees clockwise. Returns result.
     *
     * @param digit Indexing digit (between 1 and 6 inclusive)
     */
    public static int rotate60cw(int digit) {
        return switch (digit) {
            case 1 -> // K_AXES_DIGIT
                Direction.JK_AXES_DIGIT.digit();
            case 3 -> // JK_AXES_DIGIT:
                Direction.J_AXES_DIGIT.digit();
            case 2 -> // J_AXES_DIGIT:
                Direction.IJ_AXES_DIGIT.digit();
            case 6 -> // IJ_AXES_DIGIT
                Direction.I_AXES_DIGIT.digit();
            case 4 -> // I_AXES_DIGIT
                Direction.IK_AXES_DIGIT.digit();
            case 5 -> // IK_AXES_DIGIT
                Direction.K_AXES_DIGIT.digit();
            default -> digit;
        };
    }

    /**
     * Rotates indexing digit 60 degrees counter-clockwise. Returns result.
     *
     * @param digit Indexing digit (between 1 and 6 inclusive)
     */
    public static int rotate60ccw(int digit) {
        return switch (digit) {
            case 1 -> // K_AXES_DIGIT
                Direction.IK_AXES_DIGIT.digit();
            case 5 -> // IK_AXES_DIGIT
                Direction.I_AXES_DIGIT.digit();
            case 4 -> // I_AXES_DIGIT
                Direction.IJ_AXES_DIGIT.digit();
            case 6 -> // IJ_AXES_DIGIT
                Direction.J_AXES_DIGIT.digit();
            case 2 -> // J_AXES_DIGIT:
                Direction.JK_AXES_DIGIT.digit();
            case 3 -> // JK_AXES_DIGIT:
                Direction.K_AXES_DIGIT.digit();
            default -> digit;
        };
    }
}
