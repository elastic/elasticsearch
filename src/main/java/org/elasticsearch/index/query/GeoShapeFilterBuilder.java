/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.spatial4j.core.shape.Shape;
import org.elasticsearch.common.geo.GeoJSONShapeSerializer;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.SpatialStrategy;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * {@link FilterBuilder} that builds a GeoShape Filter
 */
public class GeoShapeFilterBuilder extends BaseFilterBuilder {

    private final String name;

    private final Shape shape;

    private SpatialStrategy strategy = null;

    private Boolean cache;
    private String cacheKey;

    private String filterName;

    private final String indexedShapeId;
    private final String indexedShapeType;

    private String indexedShapeIndex;
    private String indexedShapeFieldName;

    private ShapeRelation relation = null;
    
    /**
     * Creates a new GeoShapeFilterBuilder whose Filter will be against the
     * given field name using the given Shape
     *
     * @param name  Name of the field that will be filtered
     * @param shape Shape used in the filter
     */
    public GeoShapeFilterBuilder(String name, Shape shape) {
        this(name, shape, null, null, null);
    }

    /**
     * Creates a new GeoShapeFilterBuilder whose Filter will be against the
     * given field name using the given Shape
     *
     * @param name  Name of the field that will be filtered
     * @param relation {@link ShapeRelation} of query and indexed shape
     * @param shape Shape used in the filter
     */
    public GeoShapeFilterBuilder(String name, Shape shape, ShapeRelation relation) {
        this(name, shape, null, null, relation);
    }

    /**
     * Creates a new GeoShapeFilterBuilder whose Filter will be against the given field name
     * and will use the Shape found with the given ID in the given type
     *
     * @param name             Name of the field that will be filtered
     * @param indexedShapeId   ID of the indexed Shape that will be used in the Filter
     * @param indexedShapeType Index type of the indexed Shapes
     */
    public GeoShapeFilterBuilder(String name, String indexedShapeId, String indexedShapeType, ShapeRelation relation) {
        this(name, null, indexedShapeId, indexedShapeType, relation);
    }

    private GeoShapeFilterBuilder(String name, Shape shape, String indexedShapeId, String indexedShapeType, ShapeRelation relation) {
        this.name = name;
        this.shape = shape;
        this.indexedShapeId = indexedShapeId;
        this.relation = relation;
        this.indexedShapeType = indexedShapeType;
    }

    /**
     * Sets whether the filter will be cached.
     *
     * @param cache Whether filter will be cached
     * @return this
     */
    public GeoShapeFilterBuilder cache(boolean cache) {
        this.cache = cache;
        return this;
    }

    /**
     * Sets the key used for the filter if it is cached
     *
     * @param cacheKey Key for the Filter if cached
     * @return this
     */
    public GeoShapeFilterBuilder cacheKey(String cacheKey) {
        this.cacheKey = cacheKey;
        return this;
    }

    /**
     * Sets the name of the filter
     *
     * @param filterName Name of the filter
     * @return this
     */
    public GeoShapeFilterBuilder filterName(String filterName) {
        this.filterName = filterName;
        return this;
    }

    /**
     * Defines which spatial strategy will be used for building the geo shape filter. When not set, the strategy that
     * will be used will be the one that is associated with the geo shape field in the mappings.
     *
     * @param strategy The spatial strategy to use for building the geo shape filter
     * @return this
     */
    public GeoShapeFilterBuilder strategy(SpatialStrategy strategy) {
        this.strategy = strategy;
        return this;
    }

    /**
     * Sets the name of the index where the indexed Shape can be found
     *
     * @param indexedShapeIndex Name of the index where the indexed Shape is
     * @return this
     */
    public GeoShapeFilterBuilder indexedShapeIndex(String indexedShapeIndex) {
        this.indexedShapeIndex = indexedShapeIndex;
        return this;
    }

    /**
     * Sets the name of the field in the indexed Shape document that has the Shape itself
     *
     * @param indexedShapeFieldName Name of the field where the Shape itself is defined
     * @return this
     */
    public GeoShapeFilterBuilder indexedShapeFieldName(String indexedShapeFieldName) {
        this.indexedShapeFieldName = indexedShapeFieldName;
        return this;
    }

    /**
     * Sets the relation of query shape and indexed shape.
     *
     * @param relation relation of the shapes
     * @return this
     */
    public GeoShapeFilterBuilder relation(ShapeRelation relation) {
        this.relation = relation;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(GeoShapeFilterParser.NAME);

        builder.startObject(name);

        if (strategy != null) {
            builder.field("strategy", strategy.getStrategyName());
        }

        if (shape != null) {
            builder.startObject("shape");
            GeoJSONShapeSerializer.serialize(shape, builder);
            builder.endObject();
        } else {
            builder.startObject("indexed_shape")
                    .field("id", indexedShapeId)
                    .field("type", indexedShapeType);
            if (indexedShapeIndex != null) {
                builder.field("index", indexedShapeIndex);
            }
            if (indexedShapeFieldName != null) {
                builder.field("shape_field_name", indexedShapeFieldName);
            }
            builder.endObject();
        }

        if(relation != null) {
            builder.field("relation", relation.getRelationName());
        }

        builder.endObject();

        if (name != null) {
            builder.field("_name", filterName);
        }
        if (cache != null) {
            builder.field("_cache", cache);
        }
        if (cacheKey != null) {
            builder.field("_cache_key", cacheKey);
        }

        builder.endObject();
    }
}
