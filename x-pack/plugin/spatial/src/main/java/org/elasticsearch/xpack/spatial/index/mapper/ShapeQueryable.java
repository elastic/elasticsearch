/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial.index.mapper;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.SpatialStrategy;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.index.query.QueryShardContext;

/**
 * Implemented by {@link org.elasticsearch.index.mapper.MappedFieldType} that support
 * shape queries.
*/
public interface ShapeQueryable {

    QueryProcessor geometryQueryBuilder();

    /**
     * interface representing a query builder that generates a query from the given geometry
     */
    interface QueryProcessor {
        Query process(Geometry shape, String fieldName, ShapeRelation relation, QueryShardContext context);

        @Deprecated
        default Query process(Geometry shape, String fieldName, SpatialStrategy strategy, ShapeRelation relation,
                              QueryShardContext context) {
            return process(shape, fieldName, relation, context);
        }
    }
}
