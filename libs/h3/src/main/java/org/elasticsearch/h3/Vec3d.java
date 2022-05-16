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
 * Copyright 2018, 2020-2021 Uber Technologies, Inc.
 */

package org.elasticsearch.h3;

final class Vec3d {

    /** icosahedron face centers in x/y/z on the unit sphere */
    public static final double[][] faceCenterPoint = new double[][] {
        { 0.2199307791404606, 0.6583691780274996, 0.7198475378926182 },     // face 0
        { -0.2139234834501421, 0.1478171829550703, 0.9656017935214205 },    // face 1
        { 0.1092625278784797, -0.4811951572873210, 0.8697775121287253 },    // face 2
        { 0.7428567301586791, -0.3593941678278028, 0.5648005936517033 },    // face 3
        { 0.8112534709140969, 0.3448953237639384, 0.4721387736413930 },     // face 4
        { -0.1055498149613921, 0.9794457296411413, 0.1718874610009365 },    // face 5
        { -0.8075407579970092, 0.1533552485898818, 0.5695261994882688 },    // face 6
        { -0.2846148069787907, -0.8644080972654206, 0.4144792552473539 },   // face 7
        { 0.7405621473854482, -0.6673299564565524, -0.0789837646326737 },   // face 8
        { 0.8512303986474293, 0.4722343788582681, -0.2289137388687808 },    // face 9
        { -0.7405621473854481, 0.6673299564565524, 0.0789837646326737 },    // face 10
        { -0.8512303986474292, -0.4722343788582682, 0.2289137388687808 },   // face 11
        { 0.1055498149613919, -0.9794457296411413, -0.1718874610009365 },   // face 12
        { 0.8075407579970092, -0.1533552485898819, -0.5695261994882688 },   // face 13
        { 0.2846148069787908, 0.8644080972654204, -0.4144792552473539 },    // face 14
        { -0.7428567301586791, 0.3593941678278027, -0.5648005936517033 },   // face 15
        { -0.8112534709140971, -0.3448953237639382, -0.4721387736413930 },  // face 16
        { -0.2199307791404607, -0.6583691780274996, -0.7198475378926182 },  // face 17
        { 0.2139234834501420, -0.1478171829550704, -0.9656017935214205 },   // face 18
        { -0.1092625278784796, 0.4811951572873210, -0.8697775121287253 },   // face 19
    };

    private final double x;
    private final double y;
    private final double z;

    Vec3d(LatLng latLng) {
        double r = Math.cos(latLng.getLatRad());
        this.z = Math.sin(latLng.getLatRad());
        this.x = Math.cos(latLng.getLonRad()) * r;
        this.y = Math.sin(latLng.getLonRad()) * r;
    }

    /**
     * Calculate the square of the distance between two 3D coordinates.
     *
     * @param v The first 3D coordinate.
     * @return The square of the distance between the given points.
     */
    public double pointSquareDist(double[] v) {
        return square(x - v[0]) + square(y - v[1]) + square(z - v[2]);
    }

    /**
     * Square of a number
     *
     * @param x The input number.
     * @return The square of the input number.
     */
    private double square(double x) {
        return x * x;
    }

}
