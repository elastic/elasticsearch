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
package org.apache.lucene.queries;

import org.apache.lucene.geo.Polygon;
import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;

import java.io.IOException;
import java.util.Objects;

/**
 * Most of the Lucene's geo queries don't expose public getters, when post processing query tree (like the percolator does)
 * then this doesn't allow access to the properties that are needed to create a bounding box around the geo shape.
 *
 * This query exists to help with providing access to the bounding box of geo shapes of queries that don't expose public
 * getters to the specific properties to create a bounding box. This query doesn't add any functionality and wraps the
 * actual produced query during parsing and that then gets returned when a Lucene rewrite is triggered.
 */
public final class BoundingBoxQueryWrapper extends Query {

    private final Query query;
    private final String field;
    private final Rectangle boundingBox;

    public BoundingBoxQueryWrapper(Query query, String field, double latitude, double longitude, double radiusMeters) {
        this.query = query;
        this.field = field;
        this.boundingBox = Rectangle.fromPointDistance(latitude, longitude, radiusMeters);
    }

    public BoundingBoxQueryWrapper(Query query, String field, Polygon... polygons) {
        this.query = query;
        this.field = field;
        this.boundingBox = Rectangle.fromPolygon(polygons);
    }

    public Query getQuery() {
        return query;
    }

    public String getField() {
        return field;
    }

    public Rectangle getBoundingBox() {
        return boundingBox;
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        return query;
    }

    @Override
    public String toString(String field) {
        return "{bounding_box=" + boundingBox + ",query=" + query.toString(field) + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BoundingBoxQueryWrapper that = (BoundingBoxQueryWrapper) o;
        return Objects.equals(query, that.query);
    }

    @Override
    public int hashCode() {
        return Objects.hash(query);
    }
}
