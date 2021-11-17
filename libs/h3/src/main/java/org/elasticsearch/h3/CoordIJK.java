/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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

    /** CoordIJK unit vectors corresponding to the 7 H3 digits.
     */
    private static final int UNIT_VECS[][] = {
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
     * Find the center point in 2D cartesian coordinates of a hex.
     *
     */
    public Vec2d ijkToHex2d() {
        int i = this.i - this.k;
        int j = this.j - this.k;
        return new Vec2d(i - 0.5 * j, j * Constants.M_SQRT3_2);
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
     * to the smallest possible values.
     */
    public void ijkNormalize() {
        // remove any negative values
        if (i < 0) {
            j -= i;
            k -= i;
            i = 0;
        }

        if (j < 0) {
            i -= j;
            k -= j;
            j = 0;
        }

        if (k < 0) {
            i -= k;
            j -= k;
            k = 0;
        }

        // remove the min value if needed
        int min = i;
        if (j < min) {
            min = j;
        }
        if (k < min) {
            min = k;
        }
        if (min > 0) {
            i -= min;
            j -= min;
            k -= min;
        }
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
        final int i = this.i * 3 + this.j * 1 + this.k * 0;
        final int j = this.i * 0 + this.j * 3 + this.k * 1;
        final int k = this.i * 1 + this.j * 0 + this.k * 3;
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
        final int i = this.i * 3 + this.j * 0 + this.k * 1;
        final int j = this.i * 1 + this.j * 3 + this.k * 0;
        final int k = this.i * 0 + this.j * 1 + this.k * 3;
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
        final int i = this.i * 2 + this.j * 1 + this.k * 0;
        final int j = this.i * 0 + this.j * 2 + this.k * 1;
        final int k = this.i * 1 + this.j * 0 + this.k * 2;
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
        final int i = this.i * 2 + this.j * 0 + this.k * 1;
        final int j = this.i * 1 + this.j * 2 + this.k * 0;
        final int k = this.i * 0 + this.j * 1 + this.k * 2;
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
        final int i = this.i * 1 + this.j * 1 + this.k * 0;
        final int j = this.i * 0 + this.j * 1 + this.k * 1;
        final int k = this.i * 1 + this.j * 0 + this.k * 1;
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
        final int i = this.i * 1 + this.j * 0 + this.k * 1;
        final int j = this.i * 1 + this.j * 1 + this.k * 0;
        final int k = this.i * 0 + this.j * 1 + this.k * 1;
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
        i = this.i - this.k;
        j = this.j - this.k;
        int i = (int) Math.round((2 * this.i + this.j) / 7.0);
        int j = (int) Math.round((3 * this.j - this.i) / 7.0);
        this.i = i;
        this.j = j;
        this.k = 0;
        ijkNormalize();
    }

    /**
     * Rotates indexing digit 60 degrees clockwise. Returns result.
     *
     * @param digit Indexing digit (between 1 and 6 inclusive)
     */
    public static int rotate60cw(int digit) {
        switch (digit) {
            case 1: // K_AXES_DIGIT
                return Direction.JK_AXES_DIGIT.digit();
            case 3: // JK_AXES_DIGIT:
                return Direction.J_AXES_DIGIT.digit();
            case 2: // J_AXES_DIGIT:
                return Direction.IJ_AXES_DIGIT.digit();
            case 6: // IJ_AXES_DIGIT
                return Direction.I_AXES_DIGIT.digit();
            case 4: // I_AXES_DIGIT
                return Direction.IK_AXES_DIGIT.digit();
            case 5: // IK_AXES_DIGIT
                return Direction.K_AXES_DIGIT.digit();
            default:
                return digit;
        }
    }

}
