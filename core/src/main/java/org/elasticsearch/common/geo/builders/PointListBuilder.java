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

import com.vividsolutions.jts.geom.Coordinate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * A builder for a list of points (of {@link Coordinate} type).
 * Enables chaining of individual points either as long/lat pairs
 * or as {@link Coordinate} elements, arrays or collections.
 */
public class PointListBuilder {

    private final List<Coordinate> points = new ArrayList<>();

    /**
     * Add a new point to the collection
     * @param longitude longitude of the coordinate
     * @param latitude latitude of the coordinate
     * @return this
     */
    public PointListBuilder point(double longitude, double latitude) {
        return this.point(new Coordinate(longitude, latitude));
    }

    /**
     * Add a new point to the collection
     * @param coordinate coordinate of the point
     * @return this
     */
    public PointListBuilder point(Coordinate coordinate) {
        this.points.add(coordinate);
        return this;
    }

    /**
     * Add a array of points to the collection
     *
     * @param coordinates array of {@link Coordinate}s to add
     * @return this
     */
    public PointListBuilder points(Coordinate...coordinates) {
        return this.points(Arrays.asList(coordinates));
    }

    /**
     * Add a collection of points to the collection
     *
     * @param coordinates array of {@link Coordinate}s to add
     * @return this
     */
    public PointListBuilder points(Collection<? extends Coordinate> coordinates) {
        this.points.addAll(coordinates);
        return this;
    }

    /**
     * Closes the current list of points by adding the starting point as the end point
     * if they are not already the same
     */
    public PointListBuilder close() {
        Coordinate start = points.get(0);
        Coordinate end = points.get(points.size()-1);
        if(start.x != end.x || start.y != end.y) {
            points.add(start);
        }
        return this;
    }

    /**
     * @return the current list of points
     */
    public List<Coordinate> list() {
        return new ArrayList<>(this.points);
    }
}
