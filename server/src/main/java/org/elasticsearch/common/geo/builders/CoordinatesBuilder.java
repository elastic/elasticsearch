/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.geo.builders;

import org.locationtech.jts.geom.Coordinate;
import org.elasticsearch.ElasticsearchException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * A builder for a list of coordinates.
 * Enables chaining of individual coordinates either as long/lat pairs
 * or as {@link Coordinate} elements, arrays or collections.
 */
public class CoordinatesBuilder {

    private final List<Coordinate> points = new ArrayList<>();

    /**
     * Add a new coordinate to the collection
     * @param coordinate the coordinate to add
     * @return this
     */
    public CoordinatesBuilder coordinate(Coordinate coordinate) {
        int expectedDims;
        int actualDims;
        if (points.isEmpty() == false
                && (expectedDims = Double.isNaN(points.get(0).z) ? 2 : 3) != (actualDims = Double.isNaN(coordinate.z) ? 2 : 3)) {
            throw new ElasticsearchException("unable to add coordinate to CoordinateBuilder: " +
                "coordinate dimensions do not match. Expected [{}] but found [{}]", expectedDims, actualDims);

        } else {
            this.points.add(coordinate);
        }
        return this;
    }

    /**
     * Add a new coordinate to the collection
     * @param longitude longitude of the coordinate
     * @param latitude latitude of the coordinate
     * @return this
     */
    public CoordinatesBuilder coordinate(double longitude, double latitude) {
        return this.coordinate(new Coordinate(longitude, latitude));
    }

    /**
     * Add an array of coordinates to the current coordinates
     *
     * @param coordinates array of {@link Coordinate}s to add
     * @return this
     */
    public CoordinatesBuilder coordinates(Coordinate...coordinates) {
        return this.coordinates(Arrays.asList(coordinates));
    }

    /**
     * Add a collection of coordinates to the current coordinates
     *
     * @param coordinates collection of {@link Coordinate}s to add
     * @return this
     */
    public CoordinatesBuilder coordinates(Collection<? extends Coordinate> coordinates) {
        this.points.addAll(coordinates);
        return this;
    }

    /**
     * Makes a closed ring out of the current coordinates by adding the starting point as the end point.
     * Will have no effect of starting and end point are already the same coordinate.
     */
    public CoordinatesBuilder close() {
        Coordinate start = points.get(0);
        Coordinate end = points.get(points.size()-1);
        if(start.x != end.x || start.y != end.y) {
            points.add(start);
        }
        return this;
    }

    /**
     * @return a list containing the current coordinates
     */
    public List<Coordinate> build() {
        return new ArrayList<>(this.points);
    }
}
