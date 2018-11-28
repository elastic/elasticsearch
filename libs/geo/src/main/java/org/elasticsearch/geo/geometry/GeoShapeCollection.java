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
package org.elasticsearch.geo.geometry;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.lucene.store.OutputStreamDataOutput;

public class GeoShapeCollection extends GeoShape {
    protected GeoShape[] shapes;
    private final boolean hasArea;

    public GeoShapeCollection(GeoShape... shapes) {
        if (shapes.length < 1) {
            throw new IllegalArgumentException("must have at least one shape to create a " + type());
        }
        // nocommit - CHECK THIS
        this.shapes = shapes.clone();

        double minLat = Double.POSITIVE_INFINITY;
        double maxLat = Double.NEGATIVE_INFINITY;
        double minLon = Double.POSITIVE_INFINITY;
        double maxLon = Double.NEGATIVE_INFINITY;

        Rectangle bbox;
        boolean hasArea = false;
        for (GeoShape shape : shapes) {
            if (hasArea == false && shape.hasArea()) {
                hasArea = true;
            }
            bbox = shape.getBoundingBox();
            minLat = StrictMath.min(minLat, bbox.minLat());
            maxLat = StrictMath.max(maxLat, bbox.maxLat());
            minLon = StrictMath.min(minLon, bbox.minLon());
            maxLon = StrictMath.max(maxLon, bbox.maxLon());
        }
        this.hasArea = hasArea;
        this.boundingBox = new Rectangle(minLat, maxLat, minLon, maxLon);
    }

    @Override
    public boolean hasArea() {
        return hasArea;
    }

    @Override
    protected double computeArea() {
        double area = 0;
        for (GeoShape shape : shapes) {
            if (shape.hasArea()) {
                area += shape.getArea();
            }
        }
        return area;
    }

    @Override
    public ShapeType type() {
        return ShapeType.GEOMETRYCOLLECTION;
    }

    @Override
    public Relation relate(double minLat, double maxLat, double minLon, double maxLon) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Relation relate(GeoShape shape) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    protected StringBuilder contentToWKT() {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    protected void appendWKBContent(OutputStreamDataOutput out) throws IOException {
        throw new UnsupportedEncodingException("not yet implemented");
    }
}
