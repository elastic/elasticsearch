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

/**
 * Mutable face number and ijk coordinates on that face-centered coordinate system.
 *
 *  References the Vec2d cartesian coordinate systems hex2d: local face-centered
 *  coordinate system scaled a specific H3 grid resolution unit length and
 *  with x-axes aligned with the local i-axes
 */
final class FaceIJK {

    /** enum representing overage type */
    enum Overage {
        /**
         * Digit representing overage type
         */
        NO_OVERAGE,
        /**
         * On face edge (only occurs on substrate grids)
         */
        FACE_EDGE,
        /**
         * Overage on new face interior
         */
        NEW_FACE
    }

    // indexes for faceNeighbors table
    /**
     * IJ quadrant faceNeighbors table direction
     */
    private static final int IJ = 1;
    /**
     * KI quadrant faceNeighbors table direction
     */
    private static final int KI = 2;
    /**
     * JK quadrant faceNeighbors table direction
     */
    private static final int JK = 3;

    /**
     * overage distance table
     */
    private static final int[] maxDimByCIIres = {
        2,        // res 0
        -1,       // res 1
        14,       // res 2
        -1,       // res 3
        98,       // res 4
        -1,       // res 5
        686,      // res 6
        -1,       // res 7
        4802,     // res 8
        -1,       // res 9
        33614,    // res 10
        -1,       // res 11
        235298,   // res 12
        -1,       // res 13
        1647086,  // res 14
        -1,       // res 15
        11529602  // res 16
    };

    private static final Vec2d[][] maxDimByCIIVec2d = new Vec2d[maxDimByCIIres.length][3];
    static {
        for (int i = 0; i < maxDimByCIIres.length; i++) {
            maxDimByCIIVec2d[i][0] = new Vec2d(3.0 * maxDimByCIIres[i], 0.0);
            maxDimByCIIVec2d[i][1] = new Vec2d(-1.5 * maxDimByCIIres[i], 3.0 * Constants.M_SQRT3_2 * maxDimByCIIres[i]);
            maxDimByCIIVec2d[i][2] = new Vec2d(-1.5 * maxDimByCIIres[i], -3.0 * Constants.M_SQRT3_2 * maxDimByCIIres[i]);
        }
    }

    /**
     * unit scale distance table
     */
    private static final int[] unitScaleByCIIres = {
        1,       // res 0
        -1,      // res 1
        7,       // res 2
        -1,      // res 3
        49,      // res 4
        -1,      // res 5
        343,     // res 6
        -1,      // res 7
        2401,    // res 8
        -1,      // res 9
        16807,   // res 10
        -1,      // res 11
        117649,  // res 12
        -1,      // res 13
        823543,  // res 14
        -1,      // res 15
        5764801  // res 16
    };

    /**
     * direction from the origin face to the destination face, relative to
     * the origin face's coordinate system, or -1 if not adjacent.
     */
    private static final int[][] adjacentFaceDir = new int[][] {
        { 0, KI, -1, -1, IJ, JK, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1 },  // face 0
        { IJ, 0, KI, -1, -1, -1, JK, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1 },  // face 1
        { -1, IJ, 0, KI, -1, -1, -1, JK, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1 },  // face 2
        { -1, -1, IJ, 0, KI, -1, -1, -1, JK, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1 },  // face 3
        { KI, -1, -1, IJ, 0, -1, -1, -1, -1, JK, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1 },  // face 4
        { JK, -1, -1, -1, -1, 0, -1, -1, -1, -1, IJ, -1, -1, -1, KI, -1, -1, -1, -1, -1 },  // face 5
        { -1, JK, -1, -1, -1, -1, 0, -1, -1, -1, KI, IJ, -1, -1, -1, -1, -1, -1, -1, -1 },  // face 6
        { -1, -1, JK, -1, -1, -1, -1, 0, -1, -1, -1, KI, IJ, -1, -1, -1, -1, -1, -1, -1 },  // face 7
        { -1, -1, -1, JK, -1, -1, -1, -1, 0, -1, -1, -1, KI, IJ, -1, -1, -1, -1, -1, -1 },  // face 8
        { -1, -1, -1, -1, JK, -1, -1, -1, -1, 0, -1, -1, -1, KI, IJ, -1, -1, -1, -1, -1 },  // face 9
        { -1, -1, -1, -1, -1, IJ, KI, -1, -1, -1, 0, -1, -1, -1, -1, JK, -1, -1, -1, -1 },  // face 10
        { -1, -1, -1, -1, -1, -1, IJ, KI, -1, -1, -1, 0, -1, -1, -1, -1, JK, -1, -1, -1 },  // face 11
        { -1, -1, -1, -1, -1, -1, -1, IJ, KI, -1, -1, -1, 0, -1, -1, -1, -1, JK, -1, -1 },  // face 12
        { -1, -1, -1, -1, -1, -1, -1, -1, IJ, KI, -1, -1, -1, 0, -1, -1, -1, -1, JK, -1 },  // face 13
        { -1, -1, -1, -1, -1, KI, -1, -1, -1, IJ, -1, -1, -1, -1, 0, -1, -1, -1, -1, JK },  // face 14
        { -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, JK, -1, -1, -1, -1, 0, IJ, -1, -1, KI },  // face 15
        { -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, JK, -1, -1, -1, KI, 0, IJ, -1, -1 },  // face 16
        { -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, JK, -1, -1, -1, KI, 0, IJ, -1 },  // face 17
        { -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, JK, -1, -1, -1, KI, 0, IJ },  // face 18
        { -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, JK, IJ, -1, -1, KI, 0 }  // face 19
    };

    /** Maximum input for any component to face-to-base-cell lookup functions */
    private static final int MAX_FACE_COORD = 2;

    /**
     *  Information to transform into an adjacent face IJK system
     */
    private record FaceOrientIJK(
        int face, // face number
        int translateI, // res 0 translation relative to primary face
        int translateJ,
        int translateK,
        int ccwRot60// number of 60 degree ccw rotations relative to primary
    ) {}

    /**
     *  Definition of which faces neighbor each other.
     */
    private static final FaceOrientIJK[][] faceNeighbors = new FaceOrientIJK[][] {
        {
            // face 0
            new FaceOrientIJK(0, 0, 0, 0, 0),  // central face
            new FaceOrientIJK(4, 2, 0, 2, 1),  // ij quadrant
            new FaceOrientIJK(1, 2, 2, 0, 5),  // ki quadrant
            new FaceOrientIJK(5, 0, 2, 2, 3)   // jk quadrant
        },
        {
            // face 1
            new FaceOrientIJK(1, 0, 0, 0, 0),  // central face
            new FaceOrientIJK(0, 2, 0, 2, 1),  // ij quadrant
            new FaceOrientIJK(2, 2, 2, 0, 5),  // ki quadrant
            new FaceOrientIJK(6, 0, 2, 2, 3)   // jk quadrant
        },
        {
            // face 2
            new FaceOrientIJK(2, 0, 0, 0, 0),  // central face
            new FaceOrientIJK(1, 2, 0, 2, 1),  // ij quadrant
            new FaceOrientIJK(3, 2, 2, 0, 5),  // ki quadrant
            new FaceOrientIJK(7, 0, 2, 2, 3)   // jk quadrant
        },
        {
            // face 3
            new FaceOrientIJK(3, 0, 0, 0, 0),  // central face
            new FaceOrientIJK(2, 2, 0, 2, 1),  // ij quadrant
            new FaceOrientIJK(4, 2, 2, 0, 5),  // ki quadrant
            new FaceOrientIJK(8, 0, 2, 2, 3)   // jk quadrant
        },
        {
            // face 4
            new FaceOrientIJK(4, 0, 0, 0, 0),  // central face
            new FaceOrientIJK(3, 2, 0, 2, 1),  // ij quadrant
            new FaceOrientIJK(0, 2, 2, 0, 5),  // ki quadrant
            new FaceOrientIJK(9, 0, 2, 2, 3)   // jk quadrant
        },
        {
            // face 5
            new FaceOrientIJK(5, 0, 0, 0, 0),   // central face
            new FaceOrientIJK(10, 2, 2, 0, 3),  // ij quadrant
            new FaceOrientIJK(14, 2, 0, 2, 3),  // ki quadrant
            new FaceOrientIJK(0, 0, 2, 2, 3)    // jk quadrant
        },
        {
            // face 6
            new FaceOrientIJK(6, 0, 0, 0, 0),   // central face
            new FaceOrientIJK(11, 2, 2, 0, 3),  // ij quadrant
            new FaceOrientIJK(10, 2, 0, 2, 3),  // ki quadrant
            new FaceOrientIJK(1, 0, 2, 2, 3)    // jk quadrant
        },
        {
            // face 7
            new FaceOrientIJK(7, 0, 0, 0, 0),   // central face
            new FaceOrientIJK(12, 2, 2, 0, 3),  // ij quadrant
            new FaceOrientIJK(11, 2, 0, 2, 3),  // ki quadrant
            new FaceOrientIJK(2, 0, 2, 2, 3)    // jk quadrant
        },
        {
            // face 8
            new FaceOrientIJK(8, 0, 0, 0, 0),   // central face
            new FaceOrientIJK(13, 2, 2, 0, 3),  // ij quadrant
            new FaceOrientIJK(12, 2, 0, 2, 3),  // ki quadrant
            new FaceOrientIJK(3, 0, 2, 2, 3)    // jk quadrant
        },
        {
            // face 9
            new FaceOrientIJK(9, 0, 0, 0, 0),   // central face
            new FaceOrientIJK(14, 2, 2, 0, 3),  // ij quadrant
            new FaceOrientIJK(13, 2, 0, 2, 3),  // ki quadrant
            new FaceOrientIJK(4, 0, 2, 2, 3)   // jk quadrant
        },
        {
            // face 10
            new FaceOrientIJK(10, 0, 0, 0, 0),  // central face
            new FaceOrientIJK(5, 2, 2, 0, 3),   // ij quadrant
            new FaceOrientIJK(6, 2, 0, 2, 3),   // ki quadrant
            new FaceOrientIJK(15, 0, 2, 2, 3)   // jk quadrant
        },
        {
            // face 11
            new FaceOrientIJK(11, 0, 0, 0, 0),  // central face
            new FaceOrientIJK(6, 2, 2, 0, 3),   // ij quadrant
            new FaceOrientIJK(7, 2, 0, 2, 3),   // ki quadrant
            new FaceOrientIJK(16, 0, 2, 2, 3)   // jk quadrant
        },
        {
            // face 12
            new FaceOrientIJK(12, 0, 0, 0, 0),  // central face
            new FaceOrientIJK(7, 2, 2, 0, 3),   // ij quadrant
            new FaceOrientIJK(8, 2, 0, 2, 3),   // ki quadrant
            new FaceOrientIJK(17, 0, 2, 2, 3)   // jk quadrant
        },
        {
            // face 13
            new FaceOrientIJK(13, 0, 0, 0, 0),  // central face
            new FaceOrientIJK(8, 2, 2, 0, 3),   // ij quadrant
            new FaceOrientIJK(9, 2, 0, 2, 3),   // ki quadrant
            new FaceOrientIJK(18, 0, 2, 2, 3)   // jk quadrant
        },
        {
            // face 14
            new FaceOrientIJK(14, 0, 0, 0, 0),  // central face
            new FaceOrientIJK(9, 2, 2, 0, 3),   // ij quadrant
            new FaceOrientIJK(5, 2, 0, 2, 3),   // ki quadrant
            new FaceOrientIJK(19, 0, 2, 2, 3)   // jk quadrant
        },
        {
            // face 15
            new FaceOrientIJK(15, 0, 0, 0, 0),  // central face
            new FaceOrientIJK(16, 2, 0, 2, 1),  // ij quadrant
            new FaceOrientIJK(19, 2, 2, 0, 5),  // ki quadrant
            new FaceOrientIJK(10, 0, 2, 2, 3)   // jk quadrant
        },
        {
            // face 16
            new FaceOrientIJK(16, 0, 0, 0, 0),  // central face
            new FaceOrientIJK(17, 2, 0, 2, 1),  // ij quadrant
            new FaceOrientIJK(15, 2, 2, 0, 5),  // ki quadrant
            new FaceOrientIJK(11, 0, 2, 2, 3)   // jk quadrant
        },
        {
            // face 17
            new FaceOrientIJK(17, 0, 0, 0, 0),  // central face
            new FaceOrientIJK(18, 2, 0, 2, 1),  // ij quadrant
            new FaceOrientIJK(16, 2, 2, 0, 5),  // ki quadrant
            new FaceOrientIJK(12, 0, 2, 2, 3)   // jk quadrant
        },
        {
            // face 18
            new FaceOrientIJK(18, 0, 0, 0, 0),  // central face
            new FaceOrientIJK(19, 2, 0, 2, 1),  // ij quadrant
            new FaceOrientIJK(17, 2, 2, 0, 5),  // ki quadrant
            new FaceOrientIJK(13, 0, 2, 2, 3)   // jk quadrant
        },
        {
            // face 19
            new FaceOrientIJK(19, 0, 0, 0, 0),  // central face
            new FaceOrientIJK(15, 2, 0, 2, 1),  // ij quadrant
            new FaceOrientIJK(18, 2, 2, 0, 5),  // ki quadrant
            new FaceOrientIJK(14, 0, 2, 2, 3)   // jk quadrant
        } };

    // the vertexes of an origin-centered cell in a Class III resolution on a
    // substrate grid with aperture sequence 33r7r. The aperture 3 gets us the
    // vertices, and the 3r7r gets us to Class II.
    // vertices listed ccw from the i-axes
    private static final int[][] VERTEX_CLASSIII = new int[][] {
        { 5, 4, 0 },  // 0
        { 1, 5, 0 },  // 1
        { 0, 5, 4 },  // 2
        { 0, 1, 5 },  // 3
        { 4, 0, 5 },  // 4
        { 5, 0, 1 }   // 5
    };

    // the vertexes of an origin-centered cell in a Class II resolution on a
    // substrate grid with aperture sequence 33r. The aperture 3 gets us the
    // vertices, and the 3r gets us back to Class II.
    // vertices listed ccw from the i-axes
    private static final int[][] VERTEX_CLASSII = new int[][] {
        { 2, 1, 0 },  // 0
        { 1, 2, 0 },  // 1
        { 0, 2, 1 },  // 2
        { 0, 1, 2 },  // 3
        { 1, 0, 2 },  // 4
        { 2, 0, 1 }   // 5
    };

    int face;        // face number
    CoordIJK coord;  // ijk coordinates on that face

    FaceIJK(int face, CoordIJK coord) {
        this.face = face;
        this.coord = coord;
    }

    /**
     * Adjusts this FaceIJK address so that the resulting cell address is
     * relative to the correct icosahedral face.
     *
     * @param res          The H3 resolution of the cell.
     * @param pentLeading4 Whether or not the cell is a pentagon with a leading
     *                     digit 4.
     * @param substrate    Whether or not the cell is in a substrate grid.
     * @return 0 if on original face (no overage); 1 if on face edge (only occurs
     * on substrate grids); 2 if overage on new face interior
     */
    public Overage adjustOverageClassII(int res, boolean pentLeading4, boolean substrate) {
        Overage overage = Overage.NO_OVERAGE;
        // get the maximum dimension value; scale if a substrate grid
        int maxDim = maxDimByCIIres[res];
        if (substrate) {
            maxDim *= 3;
        }

        // check for overage
        if (substrate && this.coord.i + this.coord.j + this.coord.k == maxDim) { // on edge
            overage = Overage.FACE_EDGE;
        } else if (this.coord.i + this.coord.j + this.coord.k > maxDim) { // overage
            overage = Overage.NEW_FACE;
            final FaceOrientIJK fijkOrient;
            if (this.coord.k > 0) {
                if (this.coord.j > 0) { // jk "quadrant"
                    fijkOrient = faceNeighbors[this.face][JK];
                } else { // ik "quadrant"
                    fijkOrient = faceNeighbors[this.face][KI];
                    // adjust for the pentagonal missing sequence
                    if (pentLeading4) {
                        // translate origin to center of pentagon
                        this.coord.ijkSub(maxDim, 0, 0);
                        // rotate to adjust for the missing sequence
                        this.coord.ijkRotate60cw();
                        // translate the origin back to the center of the triangle
                        this.coord.ijkAdd(maxDim, 0, 0);
                    }
                }
            } else { // ij "quadrant"
                fijkOrient = faceNeighbors[this.face][IJ];
            }

            this.face = fijkOrient.face;

            // rotate and translate for adjacent face
            for (int i = 0; i < fijkOrient.ccwRot60; i++) {
                this.coord.ijkRotate60ccw();
            }

            int unitScale = unitScaleByCIIres[res];
            if (substrate) {
                unitScale *= 3;
            }
            this.coord.ijkAdd(fijkOrient.translateI * unitScale, fijkOrient.translateJ * unitScale, fijkOrient.translateK * unitScale);
            this.coord.ijkNormalize();

            // overage points on pentagon boundaries can end up on edges
            if (substrate && this.coord.i + this.coord.j + this.coord.k == maxDim) { // on edge
                overage = Overage.FACE_EDGE;
            }
        }
        return overage;
    }

    /**
     * Computes the center point in spherical coordinates of a cell given by
     * a FaceIJK address at a specified resolution.
     *
     * @param res The H3 resolution of the cell.
     */
    public LatLng faceIjkToGeo(int res) {
        return coord.ijkToGeo(face, res, false);
    }

    /**
     * Computes the cell boundary in spherical coordinates for a pentagonal cell
     * for this FaceIJK address at a specified resolution.
     *
     * @param res    The H3 resolution of the cell.
     */
    public CellBoundary faceIjkPentToCellBoundary(int res) {
        // adjust the center point to be in an aperture 33r substrate grid
        // these should be composed for speed
        this.coord.downAp3();
        this.coord.downAp3r();
        // if res is Class III we need to add a cw aperture 7 to get to
        // icosahedral Class II
        final int adjRes = adjustRes(this.coord, res);

        // If we're returning the entire loop, we need one more iteration in case
        // of a distortion vertex on the last edge
        if (H3Index.isResolutionClassIII(res)) {
            return faceIjkPentToCellBoundaryClassIII(adjRes);
        } else {
            return faceIjkPentToCellBoundaryClassII(adjRes);
        }
    }

    private CellBoundary faceIjkPentToCellBoundaryClassII(int adjRes) {
        final LatLng[] points = new LatLng[Constants.NUM_PENT_VERTS];
        final FaceIJK fijk = new FaceIJK(this.face, new CoordIJK(0, 0, 0));
        for (int vert = 0; vert < Constants.NUM_PENT_VERTS; vert++) {
            // The center point is now in the same substrate grid as the origin
            // cell vertices. Add the center point substate coordinates
            // to each vertex to translate the vertices to that cell.
            fijk.coord.reset(
                VERTEX_CLASSII[vert][0] + this.coord.i,
                VERTEX_CLASSII[vert][1] + this.coord.j,
                VERTEX_CLASSII[vert][2] + this.coord.k
            );
            fijk.coord.ijkNormalize();
            fijk.face = this.face;

            fijk.adjustPentVertOverage(adjRes);

            points[vert] = fijk.coord.ijkToGeo(fijk.face, adjRes, true);
        }
        return new CellBoundary(points, Constants.NUM_PENT_VERTS);
    }

    private CellBoundary faceIjkPentToCellBoundaryClassIII(int adjRes) {
        final LatLng[] points = new LatLng[CellBoundary.MAX_CELL_BNDRY_VERTS];
        int numPoints = 0;
        final FaceIJK fijk = new FaceIJK(this.face, new CoordIJK(0, 0, 0));
        final CoordIJK lastCoord = new CoordIJK(0, 0, 0);
        int lastFace = this.face;
        for (int vert = 0; vert < Constants.NUM_PENT_VERTS + 1; vert++) {
            final int v = vert % Constants.NUM_PENT_VERTS;
            // The center point is now in the same substrate grid as the origin
            // cell vertices. Add the center point substate coordinates
            // to each vertex to translate the vertices to that cell.
            fijk.coord.reset(
                VERTEX_CLASSIII[v][0] + this.coord.i,
                VERTEX_CLASSIII[v][1] + this.coord.j,
                VERTEX_CLASSIII[v][2] + this.coord.k
            );
            fijk.coord.ijkNormalize();
            fijk.face = this.face;

            fijk.adjustPentVertOverage(adjRes);

            // all Class III pentagon edges cross icosa edges
            // note that Class II pentagons have vertices on the edge,
            // not edge intersections
            if (vert > 0) {
                // find hex2d of the two vertexes on the last face
                final Vec2d orig2d0 = lastCoord.ijkToHex2d();

                final int currentToLastDir = adjacentFaceDir[fijk.face][lastFace];
                final FaceOrientIJK fijkOrient = faceNeighbors[fijk.face][currentToLastDir];

                lastCoord.reset(fijk.coord.i, fijk.coord.j, fijk.coord.k);
                // rotate and translate for adjacent face
                for (int i = 0; i < fijkOrient.ccwRot60; i++) {
                    lastCoord.ijkRotate60ccw();
                }

                final int unitScale = unitScaleByCIIres[adjRes] * 3;
                lastCoord.ijkAdd(fijkOrient.translateI * unitScale, fijkOrient.translateJ * unitScale, fijkOrient.translateK * unitScale);
                lastCoord.ijkNormalize();

                final Vec2d orig2d1 = lastCoord.ijkToHex2d();

                // find the intersection and add the lat/lng point to the result
                final Vec2d inter = findIntersectionPoint(orig2d0, orig2d1, adjRes, adjacentFaceDir[fijkOrient.face][fijk.face]);
                if (inter != null) {
                    points[numPoints++] = inter.hex2dToGeo(fijkOrient.face, adjRes, true);
                }
            }

            // convert vertex to lat/lng and add to the result
            // vert == start + NUM_PENT_VERTS is only used to test for possible
            // intersection on last edge
            if (vert < Constants.NUM_PENT_VERTS) {
                points[numPoints++] = fijk.coord.ijkToGeo(fijk.face, adjRes, true);
            }
            lastFace = fijk.face;
            lastCoord.reset(fijk.coord.i, fijk.coord.j, fijk.coord.k);
        }
        return new CellBoundary(points, numPoints);
    }

    /**
     * Generates the cell boundary in spherical coordinates for a cell given by this
     * FaceIJK address at a specified resolution.
     *
     * @param res    The H3 resolution of the cell.
     */
    public CellBoundary faceIjkToCellBoundary(final int res) {
        // adjust the center point to be in an aperture 33r substrate grid
        // these should be composed for speed
        this.coord.downAp3();
        this.coord.downAp3r();

        // if res is Class III we need to add a cw aperture 7 to get to
        // icosahedral Class II
        final int adjRes = adjustRes(this.coord, res);

        // convert each vertex to lat/lng
        // adjust the face of each vertex as appropriate and introduce
        // edge-crossing vertices as needed
        if (H3Index.isResolutionClassIII(res)) {
            return faceIjkToCellBoundaryClassIII(adjRes);
        } else {
            return faceIjkToCellBoundaryClassII(adjRes);
        }
    }

    private static int adjustRes(CoordIJK coord, int res) {
        if (H3Index.isResolutionClassIII(res)) {
            coord.downAp7r();
            res += 1;
        }
        return res;
    }

    private CellBoundary faceIjkToCellBoundaryClassII(int adjRes) {
        final LatLng[] points = new LatLng[Constants.NUM_HEX_VERTS];
        final FaceIJK fijk = new FaceIJK(this.face, new CoordIJK(0, 0, 0));
        for (int vert = 0; vert < Constants.NUM_HEX_VERTS; vert++) {
            fijk.coord.reset(
                VERTEX_CLASSII[vert][0] + this.coord.i,
                VERTEX_CLASSII[vert][1] + this.coord.j,
                VERTEX_CLASSII[vert][2] + this.coord.k
            );
            fijk.coord.ijkNormalize();
            fijk.face = this.face;

            fijk.adjustOverageClassII(adjRes, false, true);

            // convert vertex to lat/lng and add to the result
            // vert == start + NUM_HEX_VERTS is only used to test for possible
            // intersection on last edge
            points[vert] = fijk.coord.ijkToGeo(fijk.face, adjRes, true);
        }
        return new CellBoundary(points, Constants.NUM_HEX_VERTS);
    }

    private CellBoundary faceIjkToCellBoundaryClassIII(int adjRes) {
        final LatLng[] points = new LatLng[CellBoundary.MAX_CELL_BNDRY_VERTS];
        int numPoints = 0;
        final FaceIJK fijk = new FaceIJK(this.face, new CoordIJK(0, 0, 0));
        final CoordIJK scratch = new CoordIJK(0, 0, 0);
        int lastFace = -1;
        Overage lastOverage = Overage.NO_OVERAGE;
        for (int vert = 0; vert < Constants.NUM_HEX_VERTS + 1; vert++) {
            final int v = vert % Constants.NUM_HEX_VERTS;
            fijk.coord.reset(
                VERTEX_CLASSIII[v][0] + this.coord.i,
                VERTEX_CLASSIII[v][1] + this.coord.j,
                VERTEX_CLASSIII[v][2] + this.coord.k
            );
            fijk.coord.ijkNormalize();
            fijk.face = this.face;

            final Overage overage = fijk.adjustOverageClassII(adjRes, false, true);

            /*
            Check for edge-crossing. Each face of the underlying icosahedron is a
            different projection plane. So if an edge of the hexagon crosses an
            icosahedron edge, an additional vertex must be introduced at that
            intersection point. Then each half of the cell edge can be projected
            to geographic coordinates using the appropriate icosahedron face
            projection. Note that Class II cell edges have vertices on the face
            edge, with no edge line intersections.
            */
            if (vert > 0 && fijk.face != lastFace && lastOverage != Overage.FACE_EDGE) {
                // find hex2d of the two vertexes on original face
                final int lastV = (v + 5) % Constants.NUM_HEX_VERTS;
                // The center point is now in the same substrate grid as the origin
                // cell vertices. Add the center point substate coordinates
                // to each vertex to translate the vertices to that cell.
                final Vec2d orig2d0 = orig(scratch, VERTEX_CLASSIII[lastV]);
                final Vec2d orig2d1 = orig(scratch, VERTEX_CLASSIII[v]);

                // find the appropriate icosa face edge vertexes
                final int face2 = ((lastFace == this.face) ? fijk.face : lastFace);
                // find the intersection and add the lat/lng point to the result
                final Vec2d inter = findIntersectionPoint(orig2d0, orig2d1, adjRes, adjacentFaceDir[this.face][face2]);
                if (inter != null) {
                    points[numPoints++] = inter.hex2dToGeo(this.face, adjRes, true);
                }
            }

            // convert vertex to lat/lng and add to the result
            // vert == start + NUM_HEX_VERTS is only used to test for possible
            // intersection on last edge
            if (vert < Constants.NUM_HEX_VERTS) {
                points[numPoints++] = fijk.coord.ijkToGeo(fijk.face, adjRes, true);
            }
            lastFace = fijk.face;
            lastOverage = overage;
        }
        return new CellBoundary(points, numPoints);
    }

    private Vec2d orig(CoordIJK scratch, int[] vertexLast) {
        scratch.reset(vertexLast[0] + this.coord.i, vertexLast[1] + this.coord.j, vertexLast[2] + this.coord.k);
        scratch.ijkNormalize();
        return scratch.ijkToHex2d();
    }

    private Vec2d findIntersectionPoint(Vec2d orig2d0, Vec2d orig2d1, int adjRes, int faceDir) {
        // find the appropriate icosa face edge vertexes
        final Vec2d edge0;
        final Vec2d edge1;
        switch (faceDir) {
            case IJ -> {
                edge0 = maxDimByCIIVec2d[adjRes][0];
                edge1 = maxDimByCIIVec2d[adjRes][1];
            }
            case JK -> {
                edge0 = maxDimByCIIVec2d[adjRes][1];
                edge1 = maxDimByCIIVec2d[adjRes][2];
            }
            // case KI:
            default -> {
                assert (faceDir == KI);
                edge0 = maxDimByCIIVec2d[adjRes][2];
                edge1 = maxDimByCIIVec2d[adjRes][0];
            }
        }
        // find the intersection and add the lat/lng point to the result
        final Vec2d inter = Vec2d.v2dIntersect(orig2d0, orig2d1, edge0, edge1);
        /*
        If a point of intersection occurs at a hexagon vertex, then each
        adjacent hexagon edge will lie completely on a single icosahedron
        face, and no additional vertex is required.
        */
        return orig2d0.numericallyIdentical(inter) || orig2d1.numericallyIdentical(inter) ? null : inter;
    }

    /**
     * compute the corresponding H3Index.
     * @param res The cell resolution.
     * @param face The face.
     * @param coord The CoordIJK.
     * @return The encoded H3Index
     */
    static long faceIjkToH3(int res, int face, CoordIJK coord) {
        // initialize the index
        long h = H3Index.H3_INIT;
        h = H3Index.H3_set_mode(h, Constants.H3_CELL_MODE);
        h = H3Index.H3_set_resolution(h, res);

        // check for res 0/base cell
        if (res == 0) {
            if (coord.i > MAX_FACE_COORD || coord.j > MAX_FACE_COORD || coord.k > MAX_FACE_COORD) {
                // out of range input
                throw new IllegalArgumentException(" out of range input");
            }
            return H3Index.H3_set_base_cell(h, BaseCells.getBaseCell(face, coord));
        }

        // we need to find the correct base cell CoordIJK for this H3 index;
        // start with the passed in face and resolution res ijk coordinates
        // in that face's coordinate system

        // build the H3Index from finest res up
        // adjust r for the fact that the res 0 base cell offsets the indexing
        // digits
        final CoordIJK scratch = new CoordIJK(0, 0, 0);
        for (int r = res; r > 0; r--) {
            final int lastI = coord.i;
            final int lastJ = coord.j;
            final int lastK = coord.k;
            if (H3Index.isResolutionClassIII(r)) {
                // rotate ccw
                coord.upAp7();
                scratch.reset(coord.i, coord.j, coord.k);
                scratch.downAp7();
            } else {
                // rotate cw
                coord.upAp7r();
                scratch.reset(coord.i, coord.j, coord.k);
                scratch.downAp7r();
            }
            scratch.reset(lastI - scratch.i, lastJ - scratch.j, lastK - scratch.k);
            scratch.ijkNormalize();
            h = H3Index.H3_set_index_digit(h, r, scratch.unitIjkToDigit());
        }

        // we should now hold the IJK of the base cell in the
        // coordinate system of the given face
        if (coord.i > MAX_FACE_COORD || coord.j > MAX_FACE_COORD || coord.k > MAX_FACE_COORD) {
            // out of range input
            throw new IllegalArgumentException(" out of range input");
        }

        // lookup the correct base cell
        final int baseCell = BaseCells.getBaseCell(face, coord);
        h = H3Index.H3_set_base_cell(h, baseCell);

        // rotate if necessary to get canonical base cell orientation
        // for this base cell
        final int numRots = BaseCells.getBaseCellCCWrot60(face, coord);
        if (BaseCells.isBaseCellPentagon(baseCell)) {
            // force rotation out of missing k-axes sub-sequence
            if (H3Index.h3LeadingNonZeroDigit(h) == CoordIJK.Direction.K_AXES_DIGIT.digit()) {
                // check for a cw/ccw offset face; default is ccw
                if (BaseCells.baseCellIsCwOffset(baseCell, face)) {
                    h = H3Index.h3Rotate60cw(h);
                } else {
                    h = H3Index.h3Rotate60ccw(h);
                }
            }

            for (int i = 0; i < numRots; i++) {
                h = H3Index.h3RotatePent60ccw(h);
            }
        } else {
            for (int i = 0; i < numRots; i++) {
                h = H3Index.h3Rotate60ccw(h);
            }
        }

        return h;
    }

    /**
     * Adjusts a FaceIJK address for a pentagon vertex in a substrate grid in
     * place so that the resulting cell address is relative to the correct
     * icosahedral face.
     *
     * @param res The H3 resolution of the cell.
     */
    private void adjustPentVertOverage(int res) {
        Overage overage;
        do {
            overage = adjustOverageClassII(res, false, true);
        } while (overage == Overage.NEW_FACE);
    }
}
