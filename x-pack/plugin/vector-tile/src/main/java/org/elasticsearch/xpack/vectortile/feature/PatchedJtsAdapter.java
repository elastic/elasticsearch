/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.xpack.vectortile.feature;

import com.wdtinc.mapbox_vector_tile.VectorTile;
import com.wdtinc.mapbox_vector_tile.adapt.jts.IUserDataConverter;
import com.wdtinc.mapbox_vector_tile.adapt.jts.JtsAdapter;
import com.wdtinc.mapbox_vector_tile.build.MvtLayerProps;
import com.wdtinc.mapbox_vector_tile.encoding.GeomCmd;
import com.wdtinc.mapbox_vector_tile.encoding.GeomCmdHdr;
import com.wdtinc.mapbox_vector_tile.encoding.MvtUtil;
import com.wdtinc.mapbox_vector_tile.encoding.ZigZag;
import com.wdtinc.mapbox_vector_tile.util.Vec2d;

import org.locationtech.jts.algorithm.CGAlgorithms;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateArrays;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Adapt JTS {@link Geometry} in MVT coordinates to 'Mapbox Vector Tile' objects.
 *
 * <p>The code is a modified version of the class {@link JtsAdapter} provided under
 * the Apache License, version 2.0. This modification contains the
 * <a href="https://github.com/wdtinc/mapbox-vector-tile-java/issues/36">bug fix</a>
 * regarding polygon orientation which has not been released.
 */
final class PatchedJtsAdapter {

    /**
     * <p>Convert a flat list of JTS {@link Geometry} to a list of vector tile features.
     * The Geometry should be in MVT coordinates.</p>
     *
     * <p>Each geometry will have its own ID.</p>
     *
     * @param flatGeoms flat list of JTS geometry to convert
     * @param layerProps layer properties for tagging features
     * @param userDataConverter convert Geometry userData to MVT feature tags
     * @see JtsAdapter#flatFeatureList(Geometry)
     */
    public static List<VectorTile.Tile.Feature> toFeatures(
        Collection<Geometry> flatGeoms,
        MvtLayerProps layerProps,
        IUserDataConverter userDataConverter
    ) {

        // Guard: empty geometry
        if (flatGeoms.isEmpty()) {
            return Collections.emptyList();
        }

        final List<VectorTile.Tile.Feature> features = new ArrayList<>();
        final Vec2d cursor = new Vec2d();

        VectorTile.Tile.Feature nextFeature;

        for (Geometry nextGeom : flatGeoms) {
            cursor.set(0d, 0d);
            nextFeature = toFeature(nextGeom, cursor, layerProps, userDataConverter);
            if (nextFeature != null) {
                features.add(nextFeature);
            }
        }

        return features;
    }

    /**
     * Create and return a feature from a geometry. Returns null on failure.
     *
     * @param geom flat geometry via {@link JtsAdapter#flatFeatureList(Geometry)} that can be translated to a feature
     * @param cursor vector tile cursor position
     * @param layerProps layer properties for tagging features
     * @return new tile feature instance, or null on failure
     */
    private static VectorTile.Tile.Feature toFeature(
        Geometry geom,
        Vec2d cursor,
        MvtLayerProps layerProps,
        IUserDataConverter userDataConverter
    ) {

        // Guard: UNKNOWN Geometry
        final VectorTile.Tile.GeomType mvtGeomType = JtsAdapter.toGeomType(geom);
        if (mvtGeomType == VectorTile.Tile.GeomType.UNKNOWN) {
            return null;
        }

        final VectorTile.Tile.Feature.Builder featureBuilder = VectorTile.Tile.Feature.newBuilder();
        final boolean mvtClosePath = MvtUtil.shouldClosePath(mvtGeomType);
        final List<Integer> mvtGeom = new ArrayList<>();

        featureBuilder.setType(mvtGeomType);

        if (geom instanceof Point || geom instanceof MultiPoint) {

            // Encode as MVT point or multipoint
            mvtGeom.addAll(ptsToGeomCmds(geom, cursor));

        } else if (geom instanceof LineString || geom instanceof MultiLineString) {

            // Encode as MVT linestring or multi-linestring
            for (int i = 0; i < geom.getNumGeometries(); ++i) {
                mvtGeom.addAll(linesToGeomCmds(geom.getGeometryN(i), mvtClosePath, cursor, 1));
            }

        } else if (geom instanceof MultiPolygon || geom instanceof Polygon) {

            // Encode as MVT polygon or multi-polygon
            for (int i = 0; i < geom.getNumGeometries(); ++i) {

                final Polygon nextPoly = (Polygon) geom.getGeometryN(i);
                final List<Integer> nextPolyGeom = new ArrayList<>();
                boolean valid = true;

                // Add exterior ring
                final LineString exteriorRing = nextPoly.getExteriorRing();

                // Area must be non-zero
                final double exteriorArea = CGAlgorithms.signedArea(exteriorRing.getCoordinates());
                if (((int) Math.round(exteriorArea)) == 0) {
                    continue;
                }

                // Check CCW Winding (must be positive area in original coordinate system, MVT is positive-y-down,
                // so inequality is flipped)
                // See: https://docs.mapbox.com/vector-tiles/specification/#winding-order
                // This equality is a change from the original which is taken from
                // https://github.com/wdtinc/mapbox-vector-tile-java/pull/41
                if (exteriorArea > 0d) {
                    CoordinateArrays.reverse(exteriorRing.getCoordinates());
                }

                nextPolyGeom.addAll(linesToGeomCmds(exteriorRing, mvtClosePath, cursor, 2));

                // Add interior rings
                for (int ringIndex = 0; ringIndex < nextPoly.getNumInteriorRing(); ++ringIndex) {

                    final LineString nextInteriorRing = nextPoly.getInteriorRingN(ringIndex);

                    // Area must be non-zero
                    final double interiorArea = CGAlgorithms.signedArea(nextInteriorRing.getCoordinates());
                    if (((int) Math.round(interiorArea)) == 0) {
                        continue;
                    }

                    // Check CW Winding (must be negative area in original coordinate system, MVT is positive-y-down,
                    // so inequality is flipped)
                    // See: https://docs.mapbox.com/vector-tiles/specification/#winding-order
                    // This equality is a change from the original which is taken from
                    // https://github.com/wdtinc/mapbox-vector-tile-java/pull/41
                    if (interiorArea < 0d) {
                        CoordinateArrays.reverse(nextInteriorRing.getCoordinates());
                    }

                    // Interior ring area must be < exterior ring area, or entire geometry is invalid
                    if (Math.abs(exteriorArea) <= Math.abs(interiorArea)) {
                        valid = false;
                        break;
                    }

                    nextPolyGeom.addAll(linesToGeomCmds(nextInteriorRing, mvtClosePath, cursor, 2));
                }

                if (valid) {
                    mvtGeom.addAll(nextPolyGeom);
                }
            }
        }

        if (mvtGeom.size() < 1) {
            return null;
        }

        featureBuilder.addAllGeometry(mvtGeom);

        // Feature Properties
        userDataConverter.addTags(geom.getUserData(), layerProps, featureBuilder);

        return featureBuilder.build();
    }

    /**
     * <p>Convert a {@link Point} or {@link MultiPoint} geometry to a list of MVT geometry drawing commands. See
     * <a href="https://github.com/mapbox/vector-tile-spec">vector-tile-spec</a>
     * for details.</p>
     *
     * <p>WARNING: The value of the {@code cursor} parameter is modified as a result of calling this method.</p>
     *
     * @param geom input of type {@link Point} or {@link MultiPoint}. Type is NOT checked and expected to be correct.
     * @param cursor modified during processing to contain next MVT cursor position
     * @return list of commands
     */
    private static List<Integer> ptsToGeomCmds(final Geometry geom, final Vec2d cursor) {

        // Guard: empty geometry coordinates
        final Coordinate[] geomCoords = geom.getCoordinates();
        if (geomCoords.length <= 0) {
            return Collections.emptyList();
        }

        /** Tile commands and parameters */
        final List<Integer> geomCmds = new ArrayList<>(geomCmdBuffLenPts(geomCoords.length));

        /** Holds next MVT coordinate */
        final Vec2d mvtPos = new Vec2d();

        /** Length of 'MoveTo' draw command */
        int moveCmdLen = 0;

        // Insert placeholder for 'MoveTo' command header
        geomCmds.add(0);

        Coordinate nextCoord;

        for (int i = 0; i < geomCoords.length; ++i) {
            nextCoord = geomCoords[i];
            mvtPos.set(nextCoord.x, nextCoord.y);

            // Ignore duplicate MVT points
            if (i == 0 || equalAsInts(cursor, mvtPos) == false) {
                ++moveCmdLen;
                moveCursor(cursor, geomCmds, mvtPos);
            }
        }

        if (moveCmdLen <= GeomCmdHdr.CMD_HDR_LEN_MAX) {

            // Write 'MoveTo' command header to first index
            geomCmds.set(0, GeomCmdHdr.cmdHdr(GeomCmd.MoveTo, moveCmdLen));

            return geomCmds;

        } else {

            // Invalid geometry, need at least 1 'MoveTo' value to make points
            return Collections.emptyList();
        }
    }

    /**
     * <p>Convert a {@link LineString} or {@link Polygon} to a list of MVT geometry drawing commands.
     * A {@link MultiLineString} or {@link MultiPolygon} can be encoded by calling this method multiple times.</p>
     *
     * <p>See <a href="https://github.com/mapbox/vector-tile-spec">vector-tile-spec</a> for details.</p>
     *
     * <p>WARNING: The value of the {@code cursor} parameter is modified as a result of calling this method.</p>
     *
     * @param geom input of type {@link LineString} or {@link Polygon}. Type is NOT checked and expected to be correct.
     * @param closeEnabled whether a 'ClosePath' command should terminate the command list
     * @param cursor modified during processing to contain next MVT cursor position
     * @param minLineToLen minimum allowed length for LineTo command.
     * @return list of commands
     */
    private static List<Integer> linesToGeomCmds(
        final Geometry geom,
        final boolean closeEnabled,
        final Vec2d cursor,
        final int minLineToLen
    ) {

        final Coordinate[] geomCoords = geom.getCoordinates();

        // Check geometry for repeated end points
        final int repeatEndCoordCount = countCoordRepeatReverse(geomCoords);
        final int minExpGeomCoords = geomCoords.length - repeatEndCoordCount;

        // Guard/Optimization: Not enough geometry coordinates for a line
        if (minExpGeomCoords < 2) {
            return Collections.emptyList();
        }

        /** Tile commands and parameters */
        final List<Integer> geomCmds = new ArrayList<>(geomCmdBuffLenLines(minExpGeomCoords, closeEnabled));

        /** Holds next MVT coordinate */
        final Vec2d mvtPos = new Vec2d();

        // Initial coordinate
        Coordinate nextCoord = geomCoords[0];
        mvtPos.set(nextCoord.x, nextCoord.y);

        // Encode initial 'MoveTo' command
        geomCmds.add(GeomCmdHdr.cmdHdr(GeomCmd.MoveTo, 1));

        moveCursor(cursor, geomCmds, mvtPos);

        /** Index of 'LineTo' 'command header' */
        final int lineToCmdHdrIndex = geomCmds.size();

        // Insert placeholder for 'LineTo' command header
        geomCmds.add(0);

        /** Length of 'LineTo' draw command */
        int lineToLength = 0;

        for (int i = 1; i < minExpGeomCoords; ++i) {
            nextCoord = geomCoords[i];
            mvtPos.set(nextCoord.x, nextCoord.y);

            // Ignore duplicate MVT points in sequence
            if (equalAsInts(cursor, mvtPos) == false) {
                ++lineToLength;
                moveCursor(cursor, geomCmds, mvtPos);
            }
        }

        if (lineToLength >= minLineToLen && lineToLength <= GeomCmdHdr.CMD_HDR_LEN_MAX) {

            // Write 'LineTo' 'command header'
            geomCmds.set(lineToCmdHdrIndex, GeomCmdHdr.cmdHdr(GeomCmd.LineTo, lineToLength));

            if (closeEnabled) {
                geomCmds.add(GeomCmdHdr.closePathCmdHdr());
            }

            return geomCmds;

        } else {

            // Invalid geometry, need at least 1 'LineTo' value to make a Multiline or Polygon
            return Collections.emptyList();
        }
    }

    /**
     * <p>Count number of coordinates starting from the end of the coordinate array backwards
     * that match the first coordinate value.</p>
     *
     * <p>Useful for ensuring self-closing line strings do not repeat the first coordinate.</p>
     *
     * @param coords coordinates to check for duplicate points
     * @return number of duplicate points at the rear of the list
     */
    private static int countCoordRepeatReverse(Coordinate[] coords) {
        int repeatCoords = 0;

        final Coordinate firstCoord = coords[0];
        Coordinate nextCoord;

        for (int i = coords.length - 1; i > 0; --i) {
            nextCoord = coords[i];
            if (equalAsInts2d(firstCoord, nextCoord)) {
                ++repeatCoords;
            } else {
                break;
            }
        }

        return repeatCoords;
    }

    /**
     * <p>Appends {@link ZigZag#encode(int)} of delta in x,y from {@code cursor} to {@code mvtPos} into the {@code geomCmds} buffer.</p>
     *
     * <p>Afterwards, the {@code cursor} values are changed to match the {@code mvtPos} values.</p>
     *
     * @param cursor MVT cursor position
     * @param geomCmds geometry command list
     * @param mvtPos next MVT cursor position
     */
    private static void moveCursor(Vec2d cursor, List<Integer> geomCmds, Vec2d mvtPos) {

        // Delta, then zigzag
        geomCmds.add(ZigZag.encode((int) mvtPos.x - (int) cursor.x));
        geomCmds.add(ZigZag.encode((int) mvtPos.y - (int) cursor.y));

        cursor.set(mvtPos);
    }

    /**
     * Return true if the values of the two {@link Coordinate} are equal when their
     * first and second ordinates are cast as ints. Ignores 3rd ordinate.
     *
     * @param a first coordinate to compare
     * @param b second coordinate to compare
     * @return true if the values of the two {@link Coordinate} are equal when their
     * first and second ordinates are cast as ints
     */
    private static boolean equalAsInts2d(Coordinate a, Coordinate b) {
        return ((int) a.getOrdinate(0)) == ((int) b.getOrdinate(0)) && ((int) a.getOrdinate(1)) == ((int) b.getOrdinate(1));
    }

    /**
     * Return true if the values of the two vectors are equal when cast as ints.
     *
     * @param a first vector to compare
     * @param b second vector to compare
     * @return true if the values of the two vectors are equal when cast as ints
     */
    private static boolean equalAsInts(Vec2d a, Vec2d b) {
        return ((int) a.x) == ((int) b.x) && ((int) a.y) == ((int) b.y);
    }

    /**
     * Get required geometry buffer size for a {@link Point} or {@link MultiPoint} geometry.
     *
     * @param coordCount coordinate count for the geometry
     * @return required geometry buffer length
     */
    private static int geomCmdBuffLenPts(int coordCount) {

        // 1 MoveTo Header, 2 parameters * coordCount
        return 1 + (coordCount * 2);
    }

    /**
     * Get required geometry buffer size for a {@link LineString} or {@link Polygon} geometry.
     *
     * @param coordCount coordinate count for the geometry
     * @param closeEnabled whether a 'ClosePath' command should terminate the command list
     * @return required geometry buffer length
     */
    private static int geomCmdBuffLenLines(int coordCount, boolean closeEnabled) {

        // MoveTo Header, LineTo Header, Optional ClosePath Header, 2 parameters * coordCount
        return 2 + (closeEnabled ? 1 : 0) + (coordCount * 2);
    }
}
