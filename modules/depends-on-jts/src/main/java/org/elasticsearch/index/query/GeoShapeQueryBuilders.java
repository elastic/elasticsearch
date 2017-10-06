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

package org.elasticsearch.index.query;

import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.ShapeBuilder;

import java.io.IOException;

public class GeoShapeQueryBuilders {

    private GeoShapeQueryBuilders() {}
    
    /**
     * A filter based on the relationship of a shape and indexed shapes
     *
     * @param name  The shape field name
     * @param shape Shape to use in the filter
     */
    public static GeoShapeQueryBuilder geoShapeQuery(String name, ShapeBuilder shape) throws IOException {
        return new GeoShapeQueryBuilder(name, shape);
    }

    public static GeoShapeQueryBuilder geoShapeQuery(String name, String indexedShapeId, String indexedShapeType) {
        return new GeoShapeQueryBuilder(name, indexedShapeId, indexedShapeType);
    }

    /**
     * A filter to filter indexed shapes intersecting with shapes
     *
     * @param name  The shape field name
     * @param shape Shape to use in the filter
     */
    public static GeoShapeQueryBuilder geoIntersectionQuery(String name, ShapeBuilder shape) throws IOException {
        GeoShapeQueryBuilder builder = geoShapeQuery(name, shape);
        builder.relation(ShapeRelation.INTERSECTS);
        return builder;
    }

    public static GeoShapeQueryBuilder geoIntersectionQuery(String name, String indexedShapeId, String indexedShapeType) {
        GeoShapeQueryBuilder builder = geoShapeQuery(name, indexedShapeId, indexedShapeType);
        builder.relation(ShapeRelation.INTERSECTS);
        return builder;
    }

    /**
     * A filter to filter indexed shapes that are contained by a shape
     *
     * @param name  The shape field name
     * @param shape Shape to use in the filter
     */
    public static GeoShapeQueryBuilder geoWithinQuery(String name, ShapeBuilder shape) throws IOException {
        GeoShapeQueryBuilder builder = geoShapeQuery(name, shape);
        builder.relation(ShapeRelation.WITHIN);
        return builder;
    }

    public static GeoShapeQueryBuilder geoWithinQuery(String name, String indexedShapeId, String indexedShapeType) {
        GeoShapeQueryBuilder builder = geoShapeQuery(name, indexedShapeId, indexedShapeType);
        builder.relation(ShapeRelation.WITHIN);
        return builder;
    }

    /**
     * A filter to filter indexed shapes that are not intersection with the query shape
     *
     * @param name  The shape field name
     * @param shape Shape to use in the filter
     */
    public static GeoShapeQueryBuilder geoDisjointQuery(String name, ShapeBuilder shape) throws IOException {
        GeoShapeQueryBuilder builder = geoShapeQuery(name, shape);
        builder.relation(ShapeRelation.DISJOINT);
        return builder;
    }

    public static GeoShapeQueryBuilder geoDisjointQuery(String name, String indexedShapeId, String indexedShapeType) {
        GeoShapeQueryBuilder builder = geoShapeQuery(name, indexedShapeId, indexedShapeType);
        builder.relation(ShapeRelation.DISJOINT);
        return builder;
    }
    
}
