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
    /**
     * max H3 resolution; H3 version 1 has 16 resolutions, numbered 0 through 15
     */
    public static int MAX_H3_RES = 15;

    private static final long[] NORTH = new long[MAX_H3_RES + 1];
    private static final long[] SOUTH = new long[MAX_H3_RES + 1];
    static {
        for (int res = 0; res <= H3.MAX_H3_RES; res++) {
            NORTH[res] = H3.geoToH3(90, 0, res);
            SOUTH[res] = H3.geoToH3(-90, 0, res);
        }
    }

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

    /** returns the resolution of the provided H3 cell */
    public static int getResolution(long h3) {
        return H3Index.H3_get_resolution(h3);
    }

    /** returns the resolution of the provided H3 cell in string format */
    public static int getResolution(String h3Address) {
        return getResolution(stringToH3(h3Address));
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
        if (res < 0 || res > MAX_H3_RES) {  // LCOV_EXCL_BR_LINE
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

        for (int r = res + 1; r <= MAX_H3_RES; r++) {
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
        return Vec3d.geoToH3(res, toRadians(lat), toRadians(lng));
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
        final long[] children = new long[h3ToChildrenSize(h3)];
        for (int i = 0; i < children.length; i++) {
            children[i] = childPosToH3(h3, i);
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

    /**
     * Returns the child cell at the given position
     */
    public static long childPosToH3(long h3, int childPos) {
        final int childrenRes = H3Index.H3_get_resolution(h3) + 1;
        if (childrenRes > MAX_H3_RES) {
            throw new IllegalArgumentException("Resolution overflow");
        }
        final long childH = H3Index.H3_set_resolution(h3, childrenRes);
        if (childPos == 0) {
            return H3Index.H3_set_index_digit(childH, childrenRes, CoordIJK.Direction.CENTER_DIGIT.digit());
        }
        final boolean isPentagon = isPentagon(h3);
        final int maxPos = isPentagon ? 5 : 6;
        if (childPos < 0 || childPos > maxPos) {
            throw new IllegalArgumentException("invalid child position");
        }
        if (isPentagon) {
            // Pentagon skip digit (position) is the number 1, therefore we add one
            // to the current position.
            return H3Index.H3_set_index_digit(childH, childrenRes, childPos + 1);
        } else {
            return H3Index.H3_set_index_digit(childH, childrenRes, childPos);
        }
    }

    /**
     * Returns the child address at the given position
     */
    public static String childPosToH3(String h3Address, int childPos) {
        return h3ToString(childPosToH3(stringToH3(h3Address), childPos));
    }

    private static final int[] PEN_INTERSECTING_CHILDREN_DIRECTIONS = new int[] { 3, 1, 6, 4, 2 };
    private static final int[] HEX_INTERSECTING_CHILDREN_DIRECTIONS = new int[] { 3, 6, 2, 5, 1, 4 };

    /**
     * Returns the h3 bins on the level below which are not children of the given H3 index but
     * intersects with it.
     */
    public static long[] h3ToNoChildrenIntersecting(long h3) {
        final boolean isPentagon = isPentagon(h3);
        final long[] noChildren = new long[isPentagon ? 5 : 6];
        for (int i = 0; i < noChildren.length; i++) {
            noChildren[i] = noChildIntersectingPosToH3(h3, i);
        }
        return noChildren;
    }

    /**
     * Returns the h3 addresses on the level below which are not children of the given H3 address but
     * intersects with it.
     */
    public static String[] h3ToNoChildrenIntersecting(String h3Address) {
        return h3ToStringList(h3ToNoChildrenIntersecting(stringToH3(h3Address)));
    }

    /**
     * Returns the no child intersecting cell at the given position
     */
    public static long noChildIntersectingPosToH3(long h3, int childPos) {
        final int childrenRes = H3Index.H3_get_resolution(h3) + 1;
        if (childrenRes > MAX_H3_RES) {
            throw new IllegalArgumentException("Resolution overflow");
        }
        final boolean isPentagon = isPentagon(h3);
        final int maxPos = isPentagon ? 4 : 5;
        if (childPos < 0 || childPos > maxPos) {
            throw new IllegalArgumentException("invalid child position");
        }
        final long childH = H3Index.H3_set_resolution(h3, childrenRes);
        if (isPentagon) {
            // Pentagon skip digit (position) is the number 1, therefore we add one
            // for the skip digit and one for the 0 (center) digit.
            final long child = H3Index.H3_set_index_digit(childH, childrenRes, childPos + 2);
            return HexRing.h3NeighborInDirection(child, PEN_INTERSECTING_CHILDREN_DIRECTIONS[childPos]);
        } else {
            // we add one for the 0 (center) digit.
            final long child = H3Index.H3_set_index_digit(childH, childrenRes, childPos + 1);
            return HexRing.h3NeighborInDirection(child, HEX_INTERSECTING_CHILDREN_DIRECTIONS[childPos]);
        }
    }

    /**
     * Returns the no child intersecting cell at the given position
     */
    public static String noChildIntersectingPosToH3(String h3Address, int childPos) {
        return h3ToString(noChildIntersectingPosToH3(stringToH3(h3Address), childPos));
    }

    /**
     * Returns the neighbor indexes.
     *
     * @param h3Address Origin index
     * @return All neighbor indexes from the origin
     */
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
        final long[] ring = new long[hexRingSize(h3)];
        for (int i = 0; i < ring.length; i++) {
            ring[i] = hexRingPosToH3(h3, i);
            assert ring[i] >= 0;
        }
        return ring;
    }

    /**
     * Returns the number of neighbor indexes.
     *
     * @param h3 Origin index
     * @return the number of neighbor indexes from the origin
     */
    public static int hexRingSize(long h3) {
        return H3Index.H3_is_pentagon(h3) ? 5 : 6;
    }

    /**
     * Returns the number of neighbor indexes.
     *
     * @param h3Address Origin index
     * @return the number of neighbor indexes from the origin
     */
    public static int hexRingSize(String h3Address) {
        return hexRingSize(stringToH3(h3Address));
    }

    /**
     * Returns the neighbor index at the given position.
     *
     * @param h3 Origin index
     * @param ringPos position of the neighbour index
     * @return the actual neighbour at the given position
     */
    public static long hexRingPosToH3(long h3, int ringPos) {
        // for pentagons, we skip direction at position 2
        final int pos = H3Index.H3_is_pentagon(h3) && ringPos >= 2 ? ringPos + 1 : ringPos;
        if (pos < 0 || pos > 5) {
            throw new IllegalArgumentException("invalid ring position");
        }
        return HexRing.h3NeighborInDirection(h3, HexRing.DIRECTIONS[pos].digit());
    }

    /**
     * Returns the neighbor index at the given position.
     *
     * @param h3Address Origin index
     * @param ringPos position of the neighbour index
     * @return the actual neighbour at the given position
     */
    public static String hexRingPosToH3(String h3Address, int ringPos) {
        return h3ToString(hexRingPosToH3(stringToH3(h3Address), ringPos));
    }

    /**
     * returns whether or not the provided hexagons border
     *
     * @param origin the first index
     * @param destination the second index
     * @return whether or not the provided hexagons border
     */
    public static boolean areNeighborCells(String origin, String destination) {
        return areNeighborCells(stringToH3(origin), stringToH3(destination));
    }

    /**
     * returns whether or not the provided hexagons border
     *
     * @param origin the first index
     * @param destination the second index
     * @return whether or not the provided hexagons border
     */
    public static boolean areNeighborCells(long origin, long destination) {
        return HexRing.areNeighbours(origin, destination);
    }

    /**
     * h3ToChildrenSize returns the exact number of children for a cell at a
     * given child resolution.
     *
     * @param h3         H3Index to find the number of children of
     * @param childRes  The child resolution you're interested in
     *
     * @return long      Exact number of children (handles hexagons and pentagons
     *                  correctly)
     */
    public static long h3ToChildrenSize(long h3, int childRes) {
        final int parentRes = H3Index.H3_get_resolution(h3);
        if (childRes <= parentRes || childRes > MAX_H3_RES) {
            throw new IllegalArgumentException("Invalid child resolution [" + childRes + "]");
        }
        final int n = childRes - parentRes;
        if (H3Index.H3_is_pentagon(h3)) {
            return (1L + 5L * (_ipow(7, n) - 1L) / 6L);
        } else {
            return _ipow(7, n);
        }
    }

    /**
     * h3ToChildrenSize returns the exact number of children for a h3 affress at a
     * given child resolution.
     *
     * @param h3Address  H3 address to find the number of children of
     * @param childRes  The child resolution you're interested in
     *
     * @return int      Exact number of children (handles hexagons and pentagons
     *                  correctly)
     */
    public static long h3ToChildrenSize(String h3Address, int childRes) {
        return h3ToChildrenSize(stringToH3(h3Address), childRes);
    }

    /**
     * h3ToChildrenSize returns the exact number of children
     *
     * @param h3         H3Index to find the number of children.
     *
     * @return int      Exact number of children, 6 for Pentagons and 7 for hexagons,
     */
    public static int h3ToChildrenSize(long h3) {
        if (H3Index.H3_get_resolution(h3) == MAX_H3_RES) {
            throw new IllegalArgumentException("Invalid child resolution [" + MAX_H3_RES + "]");
        }
        return isPentagon(h3) ? 6 : 7;
    }

    /**
     * h3ToChildrenSize returns the exact number of children
     *
     * @param h3Address H3 address to find the number of children.
     *
     * @return int      Exact number of children, 6 for Pentagons and 7 for hexagons,
     */
    public static int h3ToChildrenSize(String h3Address) {
        return h3ToChildrenSize(stringToH3(h3Address));
    }

    /**
     * h3ToNotIntersectingChildrenSize returns the exact number of children intersecting
     * the given parent but not part of the children set.
     *
     * @param h3         H3Index to find the number of children.
     *
     * @return int      Exact number of children, 5 for Pentagons and 6 for hexagons,
     */
    public static int h3ToNotIntersectingChildrenSize(long h3) {
        if (H3Index.H3_get_resolution(h3) == MAX_H3_RES) {
            throw new IllegalArgumentException("Invalid child resolution [" + MAX_H3_RES + "]");
        }
        return isPentagon(h3) ? 5 : 6;
    }

    /**
     * h3ToNotIntersectingChildrenSize returns the exact number of children intersecting
     * the given parent but not part of the children set.
     *
     * @param h3Address H3 address to find the number of children.
     *
     * @return int      Exact number of children, 5 for Pentagons and 6 for hexagons,
     */
    public static int h3ToNotIntersectingChildrenSize(String h3Address) {
        return h3ToNotIntersectingChildrenSize(stringToH3(h3Address));
    }

    /**
     * Find the h3 index containing the North Pole at the given resolution.
     *
     * @param res the provided resolution.
     *
     * @return the h3 index containing the North Pole.
     */
    public static long northPolarH3(int res) {
        checkResolution(res);
        return NORTH[res];
    }

    /**
     * Find the h3 address containing the North Pole at the given resolution.
     *
     * @param res the provided resolution.
     *
     * @return the h3 address containing the North Pole.
     */
    public static String northPolarH3Address(int res) {
        return h3ToString(northPolarH3(res));
    }

    /**
     * Find the h3 index containing the South Pole at the given resolution.
     *
     * @param res the provided resolution.
     *
     * @return the h3 index containing the South Pole.
     */
    public static long southPolarH3(int res) {
        checkResolution(res);
        return SOUTH[res];
    }

    /**
     * Find the h3 address containing the South Pole at the given resolution.
     *
     * @param res the provided resolution.
     *
     * @return the h3 address containing the South Pole.
     */
    public static String southPolarH3Address(int res) {
        return h3ToString(southPolarH3(res));
    }

    /**
     * _ipow does integer exponentiation efficiently. Taken from StackOverflow.
     *
     * @param base the integer base (can be positive or negative)
     * @param exp the integer exponent (should be nonnegative)
     *
     * @return the exponentiated value
     */
    private static long _ipow(int base, int exp) {
        long result = 1;
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
        if (res < 0 || res > MAX_H3_RES) {
            throw new IllegalArgumentException("resolution [" + res + "]  is out of range (must be 0 <= res <= 15)");
        }
    }
}
