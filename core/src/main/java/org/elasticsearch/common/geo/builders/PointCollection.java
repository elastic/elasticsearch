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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.elasticsearch.common.xcontent.XContentBuilder;

import com.vividsolutions.jts.geom.Coordinate;

/**
 * The {@link PointCollection} is an abstract base implementation for all GeoShapes. It simply handles a set of points.
 */
public abstract class PointCollection<E extends PointCollection<E>> extends ShapeBuilder {

    protected final ArrayList<Coordinate> points;
    protected boolean translated = false;

    protected PointCollection() {
        this(new ArrayList<Coordinate>());
    }

    protected PointCollection(ArrayList<Coordinate> points) {
        this.points = points;
    }

    @SuppressWarnings("unchecked")
    private E thisRef() {
        return (E)this;
    }

    /**
     * Add a new point to the collection
     * @param longitude longitude of the coordinate
     * @param latitude latitude of the coordinate
     * @return this
     */
    public E point(double longitude, double latitude) {
        return this.point(coordinate(longitude, latitude));
    }

    /**
     * Add a new point to the collection
     * @param coordinate coordinate of the point
     * @return this
     */
    public E point(Coordinate coordinate) {
        this.points.add(coordinate);
        return thisRef();
    }

    /**
     * Add a array of points to the collection
     *
     * @param coordinates array of {@link Coordinate}s to add
     * @return this
     */
    public E points(Coordinate...coordinates) {
        return this.points(Arrays.asList(coordinates));
    }

    /**
     * Add a collection of points to the collection
     *
     * @param coordinates array of {@link Coordinate}s to add
     * @return this
     */
    public E points(Collection<? extends Coordinate> coordinates) {
        this.points.addAll(coordinates);
        return thisRef();
    }

    /**
     * Copy all points to a new Array
     *
     * @param closed if set to true the first point of the array is repeated as last element
     * @return Array of coordinates
     */
    protected Coordinate[] coordinates(boolean closed) {
        Coordinate[] result = points.toArray(new Coordinate[points.size() + (closed?1:0)]);
        if(closed) {
            result[result.length-1] = result[0];
        }
        return result;
    }

    /**
     * builds an array of coordinates to a {@link XContentBuilder}
     *
     * @param builder builder to use
     * @param closed repeat the first point at the end of the array if it's not already defines as last element of the array
     * @return the builder
     */
    protected XContentBuilder coordinatesToXcontent(XContentBuilder builder, boolean closed) throws IOException {
        builder.startArray();
        for(Coordinate point : points) {
            toXContent(builder, point);
        }
        if(closed) {
            Coordinate start = points.get(0);
            Coordinate end = points.get(points.size()-1);
            if(start.x != end.x || start.y != end.y) {
                toXContent(builder, points.get(0));
            }
        }
        builder.endArray();
        return builder;
    }
}
