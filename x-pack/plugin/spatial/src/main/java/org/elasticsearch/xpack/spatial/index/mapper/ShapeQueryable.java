/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.index.mapper;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.index.query.SearchExecutionContext;

/**
 * Implemented by {@link org.elasticsearch.index.mapper.MappedFieldType} that support
 * shape queries.
*/
public interface ShapeQueryable {

    Query shapeQuery(Geometry shape, String fieldName, ShapeRelation relation, SearchExecutionContext context);
}
