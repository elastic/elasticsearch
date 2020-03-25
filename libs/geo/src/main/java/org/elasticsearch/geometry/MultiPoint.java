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

package org.elasticsearch.geometry;

import java.util.List;

/**
 * Represents a MultiPoint object on the earth's surface in decimal degrees and optional altitude in meters.
 */
public class MultiPoint extends GeometryCollection<Point> {
    public static final MultiPoint EMPTY = new MultiPoint();

    private MultiPoint() {
    }

    public MultiPoint(List<Point> points) {
        super(points);
    }

    @Override
    public ShapeType type() {
        return ShapeType.MULTIPOINT;
    }

    @Override
    public <T, E extends Exception> T visit(GeometryVisitor<T, E> visitor) throws E {
        return visitor.visit(this);
    }

}
