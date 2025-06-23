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
import java.util.Objects;

/**
 * cell boundary points as {@link LatLng}
 */
public final class CellBoundary {
    /** Maximum number of cell boundary vertices; worst case is pentagon:
     *  5 original verts + 5 edge crossings
     */
    static final int MAX_CELL_BNDRY_VERTS = 10;
    /** How many points it holds */
    private final int numPoints;
    /** The actual points */
    private final LatLng[] points;

    CellBoundary(LatLng[] points, int numPoints) {
        this.points = points;
        this.numPoints = numPoints;
    }

    /** Number of points in this boundary */
    public int numPoints() {
        return numPoints;
    }

    /** Return the point at the given position*/
    public LatLng getLatLon(int i) {
        assert i >= 0 && i < numPoints;
        return points[i];
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final CellBoundary that = (CellBoundary) o;
        return numPoints == that.numPoints && Arrays.equals(points, that.points);
    }

    @Override
    public int hashCode() {
        return Objects.hash(numPoints, Arrays.hashCode(points));
    }
}
