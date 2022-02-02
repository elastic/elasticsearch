/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.SpatialStrategy;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.index.query.SearchExecutionContext;

/**
 * Implemented by {@link org.elasticsearch.index.mapper.MappedFieldType} that support
 * GeoShape queries.
 */
public interface GeoShapeQueryable {

    Query geoShapeQuery(Geometry shape, String fieldName, ShapeRelation relation, SearchExecutionContext context);

    @Deprecated
    default Query geoShapeQuery(
        Geometry shape,
        String fieldName,
        SpatialStrategy strategy,
        ShapeRelation relation,
        SearchExecutionContext context
    ) {
        return geoShapeQuery(shape, fieldName, relation, context);
    }
}
