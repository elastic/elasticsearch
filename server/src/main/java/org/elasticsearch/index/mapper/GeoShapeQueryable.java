/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.geo.LuceneGeometriesUtils;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.SpatialStrategy;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.ShapeType;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.util.function.Consumer;

/**
 * Implemented by {@link org.elasticsearch.index.mapper.MappedFieldType} that support
 * GeoShape queries.
 */
public interface GeoShapeQueryable {

    Query geoShapeQuery(SearchExecutionContext context, String fieldName, ShapeRelation relation, LatLonGeometry... luceneGeometries);

    default Query geoShapeQuery(SearchExecutionContext context, String fieldName, ShapeRelation relation, Geometry geometry) {
        final Consumer<ShapeType> checker = relation == ShapeRelation.WITHIN ? t -> {
            if (t == ShapeType.LINESTRING) {
                // Line geometries and WITHIN relation is not supported by Lucene. Throw an error here
                // to have same behavior for runtime fields.
                throw new IllegalArgumentException("found an unsupported shape Line");
            }
        } : t -> {};
        final LatLonGeometry[] luceneGeometries;
        try {
            // quantize the geometries to match the values on the index
            luceneGeometries = LuceneGeometriesUtils.toLatLonGeometry(geometry, true, checker);
        } catch (IllegalArgumentException e) {
            throw new QueryShardException(context, "Exception creating query on Field [" + fieldName + "] " + e.getMessage(), e);
        }
        if (luceneGeometries.length == 0) {
            return new MatchNoDocsQuery();
        }
        return geoShapeQuery(context, fieldName, relation, luceneGeometries);
    }

    @Deprecated
    default Query geoShapeQuery(
        SearchExecutionContext context,
        String fieldName,
        SpatialStrategy strategy,
        ShapeRelation relation,
        Geometry shape
    ) {
        return geoShapeQuery(context, fieldName, relation, shape);
    }
}
