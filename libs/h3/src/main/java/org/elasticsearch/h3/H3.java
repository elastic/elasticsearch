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

import java.util.Arrays;

import static java.lang.Math.toRadians;

/**
 * Defines the public API of the H3 library.
 */
public final class H3 {

    public static int MAX_H3_RES = Constants.MAX_H3_RES;

    /**
     * Converts from <code>long</code> representation of an index to <code>String</code> representation.
     */
    public static String h3ToString(long h3) {
        return Long.toHexString(h3);
    }

    /**
     * Converts from <code>String</code> representation of an index to <code>long</code> representation.
     */
    public static long stringToH3(String h3Address) {
        return Long.parseUnsignedLong(h3Address, 16);
    }

    /** determines if an H3 cell is a pentagon */
    public static boolean isPentagon(long h3) {
        return H3Index.H3_is_pentagon(h3);
    }

    /** determines if an H3 cell in string format is a pentagon */
    public static boolean isPentagon(String h3Address) {
        return isPentagon(stringToH3(h3Address));
    }

    /** Returns true if this is a valid H3 index */
    public static boolean h3IsValid(long h3) {
        if (H3Index.H3_get_high_bit(h3) != 0) {
            return false;
        }

        if (H3Index.H3_get_mode(h3) != Constants.H3_CELL_MODE) {
            return false;
        }

        if (H3Index.H3_get_reserved_bits(h3) != 0) {
            return false;
        }

        int baseCell = H3Index.H3_get_base_cell(h3);
        if (baseCell < 0 || baseCell >= Constants.NUM_BASE_CELLS) {  // LCOV_EXCL_BR_LINE
            // Base cells less than zero can not be represented in an index
            return false;
        }

        int res = H3Index.H3_get_resolution(h3);
        if (res < 0 || res > Constants.MAX_H3_RES) {  // LCOV_EXCL_BR_LINE
            // Resolutions less than zero can not be represented in an index
            return false;
        }

        boolean foundFirstNonZeroDigit = false;
        for (int r = 1; r <= res; r++) {
            int digit = H3Index.H3_get_index_digit(h3, r);

            if (foundFirstNonZeroDigit == false && digit != CoordIJK.Direction.CENTER_DIGIT.digit()) {
                foundFirstNonZeroDigit = true;
                if (BaseCells.isBaseCellPentagon(baseCell) && digit == CoordIJK.Direction.K_AXES_DIGIT.digit()) {
                    return false;
                }
            }

            if (digit < CoordIJK.Direction.CENTER_DIGIT.digit() || digit >= CoordIJK.Direction.NUM_DIGITS.digit()) {
                return false;
            }
        }

        for (int r = res + 1; r <= Constants.MAX_H3_RES; r++) {
            int digit = H3Index.H3_get_index_digit(h3, r);
            if (digit != CoordIJK.Direction.INVALID_DIGIT.digit()) {
                return false;
            }
        }
        return true;
    }

    /** Returns true if this is a valid H3 index */
    public static boolean h3IsValid(String h3Address) {
        return h3IsValid(stringToH3(h3Address));
    }

    /**
     * Return all base cells
     */
    public static long[] getLongRes0Cells() {
        long[] cells = new long[Constants.NUM_BASE_CELLS];
        for (int bc = 0; bc < Constants.NUM_BASE_CELLS; bc++) {
            long baseCell = H3Index.H3_INIT;
            baseCell = H3Index.H3_set_mode(baseCell, Constants.H3_CELL_MODE);
            baseCell = H3Index.H3_set_base_cell(baseCell, bc);
            cells[bc] = baseCell;
        }
        return cells;
    }

    /**
     * Return all base cells
     */
    public static String[] getStringRes0Cells() {
        return h3ToStringList(getLongRes0Cells());
    }

    /**
     * Find the {@link LatLng} center point of the cell.
     */
    public static LatLng h3ToLatLng(long h3) {
        final FaceIJK fijk = H3Index.h3ToFaceIjk(h3);
        return fijk.faceIjkToGeo(H3Index.H3_get_resolution(h3));
    }

    /**
     * Find the {@link LatLng}  center point of the cell.
     */
    public static LatLng h3ToLatLng(String h3Address) {
        return h3ToLatLng(stringToH3(h3Address));
    }

    /**
     * Find the cell {@link CellBoundary} coordinates for the cell
     */
    public static CellBoundary h3ToGeoBoundary(long h3) {
        FaceIJK fijk = H3Index.h3ToFaceIjk(h3);
        if (H3Index.H3_is_pentagon(h3)) {
            return fijk.faceIjkPentToCellBoundary(H3Index.H3_get_resolution(h3), 0, Constants.NUM_PENT_VERTS);
        } else {
            return fijk.faceIjkToCellBoundary(H3Index.H3_get_resolution(h3), 0, Constants.NUM_HEX_VERTS);
        }
    }

    /**
     * Find the cell {@link CellBoundary} coordinates for the cell
     */
    public static CellBoundary h3ToGeoBoundary(String h3Address) {
        return h3ToGeoBoundary(stringToH3(h3Address));
    }

    /**
     * Find the H3 index of the resolution <code>res</code> cell containing the lat/lon (in degrees)
     *
     * @param lat Latitude in degrees.
     * @param lng Longitude in degrees.
     * @param res Resolution, 0 &lt;= res &lt;= 15
     * @return The H3 index.
     * @throws IllegalArgumentException latitude, longitude, or resolution are out of range.
     */
    public static long geoToH3(double lat, double lng, int res) {
        checkResolution(res);
        return new LatLng(toRadians(lat), toRadians(lng)).geoToFaceIJK(res).faceIjkToH3(res);
    }

    /**
     * Find the H3 index of the resolution <code>res</code> cell containing the lat/lon (in degrees)
     *
     * @param lat Latitude in degrees.
     * @param lng Longitude in degrees.
     * @param res Resolution, 0 &lt;= res &lt;= 15
     * @return The H3 index.
     * @throws IllegalArgumentException Latitude, longitude, or resolution is out of range.
     */
    public static String geoToH3Address(double lat, double lng, int res) {
        return h3ToString(geoToH3(lat, lng, res));
    }

    /**
     * Returns the parent of the given index.
     */
    public static long h3ToParent(long h3) {
        int childRes = H3Index.H3_get_resolution(h3);
        if (childRes == 0) {
            throw new IllegalArgumentException("Input is a base cell");
        }
        long parentH = H3Index.H3_set_resolution(h3, childRes - 1);
        return H3Index.H3_set_index_digit(parentH, childRes, H3Index.H3_DIGIT_MASK);
    }

    /**
     * Returns the parent of the given index.
     */
    public static String h3ToParent(String h3Address) {
        long parent = h3ToParent(stringToH3(h3Address));
        return h3ToString(parent);
    }

    /**
     * Returns the children of the given index.
     */
    public static long[] h3ToChildren(long h3) {
        long[] children = new long[cellToChildrenSize(h3)];
        int res = H3Index.H3_get_resolution(h3);
        Iterator.IterCellsChildren it = Iterator.iterInitParent(h3, res + 1);
        int pos = 0;
        while (it.h != Iterator.H3_NULL) {
            children[pos++] = it.h;
            Iterator.iterStepChild(it);
        }
        return children;
    }

    /**
     * Transforms a list of H3 indexes in long form to a list of H3
     * indexes in string form.
     */
    public static String[] h3ToChildren(String h3Address) {
        return h3ToStringList(h3ToChildren(stringToH3(h3Address)));
    }

    public static String[] hexRing(String h3Address) {
        return h3ToStringList(hexRing(stringToH3(h3Address)));
    }

    /**
     * Returns the neighbor indexes.
     *
     * @param h3 Origin index
     * @return All neighbor indexes from the origin
     */
    public static long[] hexRing(long h3) {
        return HexRing.hexRing(h3);
    }

    /**
     * cellToChildrenSize returns the exact number of children for a cell at a
     * given child resolution.
     *
     * @param h         H3Index to find the number of children of
     *
     * @return int      Exact number of children (handles hexagons and pentagons
     *                  correctly)
     */
    private static int cellToChildrenSize(long h) {
        int n = 1;
        if (H3Index.H3_is_pentagon(h)) {
            return (1 + 5 * (_ipow(7, n) - 1) / 6);
        } else {
            return _ipow(7, n);
        }
    }

    /**
     * _ipow does integer exponentiation efficiently. Taken from StackOverflow.
     *
     * @param base the integer base (can be positive or negative)
     * @param exp the integer exponent (should be nonnegative)
     *
     * @return the exponentiated value
     */
    private static int _ipow(int base, int exp) {
        int result = 1;
        while (exp != 0) {
            if ((exp & 1) != 0) {
                result *= base;
            }
            exp >>= 1;
            base *= base;
        }

        return result;
    }

    private static String[] h3ToStringList(long[] h3s) {
        return Arrays.stream(h3s).mapToObj(H3::h3ToString).toArray(String[]::new);
    }

    /**
     * @throws IllegalArgumentException <code>res</code> is not a valid H3 resolution.
     */
    private static void checkResolution(int res) {
        if (res < 0 || res > Constants.MAX_H3_RES) {
            throw new IllegalArgumentException("resolution [" + res + "]  is out of range (must be 0 <= res <= 15)");
        }
    }
}
