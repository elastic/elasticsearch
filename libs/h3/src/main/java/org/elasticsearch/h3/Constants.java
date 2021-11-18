/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.h3;

/**
 * Constants used by more than one source code file.
 */
final class Constants {
    /**
     * sqrt(3) / 2.0
     */
    public static double M_SQRT3_2 = 0.8660254037844386467637231707529361834714;
    /**
     * max H3 resolution; H3 version 1 has 16 resolutions, numbered 0 through 15
     */
    public static int MAX_H3_RES = 15;
    /**
     * The number of H3 base cells
     */
    public static int NUM_BASE_CELLS = 122;
    /**
     * The number of vertices in a hexagon
     */
    public static int NUM_HEX_VERTS = 6;
    /**
     * The number of vertices in a pentagon
     */
    public static int NUM_PENT_VERTS = 5;
    /**
     * H3 index modes
     */
    public static int H3_CELL_MODE = 1;
    /**
     * square root of 7
     */
    public static final double M_SQRT7 = 2.6457513110645905905016157536392604257102;
    /**
     * scaling factor from hex2d resolution 0 unit length
     * (or distance between adjacent cell center points
     * on the plane) to gnomonic unit length.
     */
    public static double RES0_U_GNOMONIC = 0.38196601125010500003;
    /**
     * rotation angle between Class II and Class III resolution axes
     * (asin(sqrt(3.0 / 28.0)))
     */
    public static double M_AP7_ROT_RADS = 0.333473172251832115336090755351601070065900389;
    /**
     * threshold epsilon
     */
    public static double EPSILON = 0.0000000000000001;
}
