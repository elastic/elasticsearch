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
 * Copyright 2016-2017, 2020 Uber Technologies, Inc.
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
     * 2.0 * PI
     */
    public static final double M_2PI = 2.0 * Math.PI;
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
     * 1 / square root of 7
     */
    public static final double M_RSQRT7 = 1.0 / M_SQRT7;
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
