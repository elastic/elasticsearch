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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * The {@link CoordinateCollection} is an abstract base implementation for {@link LineStringBuilder} and {@link MultiPointBuilder}.
 * It holds a common list of {@link Coordinate}, provides setters for adding elements to the list and can render this to XContent.
 */
public abstract class CoordinateCollection<E extends CoordinateCollection<E>> extends ShapeBuilder {

    protected final List<Coordinate> coordinates;

    /**
     * Construct a new collection of coordinates.
     * @param coordinates an initial list of coordinates
     * @throws IllegalArgumentException if coordinates is <tt>null</tt> or empty
     */
    protected CoordinateCollection(List<Coordinate> coordinates) {
        if (coordinates == null || coordinates.size() == 0) {
            throw new IllegalArgumentException("cannot create point collection with empty set of points");
        }
        this.coordinates = coordinates;
    }

    /**
     * Read from a stream.
     */
    protected CoordinateCollection(StreamInput in) throws IOException {
        int size = in.readVInt();
        coordinates = new ArrayList<>(size);
        for (int i=0; i < size; i++) {
            coordinates.add(readFromStream(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(coordinates.size());
        for (Coordinate point : coordinates) {
            writeCoordinateTo(point, out);
        }
    }

    @SuppressWarnings("unchecked")
    private E thisRef() {
        return (E)this;
    }

    /**
     * Add a new coordinate to the collection
     * @param longitude longitude of the coordinate
     * @param latitude latitude of the coordinate
     * @return this
     */
    public E coordinate(double longitude, double latitude) {
        return this.coordinate(new Coordinate(longitude, latitude));
    }

    /**
     * Add a new coordinate to the collection
     * @param coordinate coordinate of the point
     * @return this
     */
    public E coordinate(Coordinate coordinate) {
        this.coordinates.add(coordinate);
        return thisRef();
    }

    /**
     * Add a array of coordinates to the collection
     *
     * @param coordinates array of {@link Coordinate}s to add
     * @return this
     */
    public E coordinates(Coordinate...coordinates) {
        return this.coordinates(Arrays.asList(coordinates));
    }

    /**
     * Add a collection of coordinates to the collection
     *
     * @param coordinates array of {@link Coordinate}s to add
     * @return this
     */
    public E coordinates(Collection<? extends Coordinate> coordinates) {
        this.coordinates.addAll(coordinates);
        return thisRef();
    }

    /**
     * Copy all coordinate to a new Array
     *
     * @param closed if set to true the first point of the array is repeated as last element
     * @return Array of coordinates
     */
    protected Coordinate[] coordinates(boolean closed) {
        Coordinate[] result = coordinates.toArray(new Coordinate[coordinates.size() + (closed?1:0)]);
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
        for(Coordinate coord : coordinates) {
            toXContent(builder, coord);
        }
        if(closed) {
            Coordinate start = coordinates.get(0);
            Coordinate end = coordinates.get(coordinates.size()-1);
            if(start.x != end.x || start.y != end.y) {
                toXContent(builder, coordinates.get(0));
            }
        }
        builder.endArray();
        return builder;
    }
}
