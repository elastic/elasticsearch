/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.index.query;

import org.apache.lucene.document.XYShape;
import org.apache.lucene.geo.XYGeometry;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.geo.LuceneGeometriesUtils;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.lucene.spatial.CartesianShapeDocValuesQuery;
import org.elasticsearch.xpack.spatial.index.mapper.ShapeFieldMapper;

public class ShapeQueryProcessor {

    public Query shapeQuery(
        Geometry geometry,
        String fieldName,
        ShapeRelation relation,
        SearchExecutionContext context,
        boolean hasDocValues
    ) {
        validateIsShapeFieldType(fieldName, context);
        // CONTAINS queries are not supported by VECTOR strategy for indices created before version 7.5.0 (Lucene 8.3.0);
        if (relation == ShapeRelation.CONTAINS && context.indexVersionCreated().before(IndexVersions.V_7_5_0)) {
            throw new QueryShardException(context, ShapeRelation.CONTAINS + " query relation not supported for Field [" + fieldName + "].");
        }
        if (geometry == null || geometry.isEmpty()) {
            return new MatchNoDocsQuery();
        }
        final XYGeometry[] luceneGeometries;
        try {
            luceneGeometries = LuceneGeometriesUtils.toXYGeometry(geometry, t -> {});
        } catch (IllegalArgumentException e) {
            throw new QueryShardException(context, "Exception creating query on Field [" + fieldName + "] " + e.getMessage(), e);
        }
        Query query = XYShape.newGeometryQuery(fieldName, relation.getLuceneRelation(), luceneGeometries);
        if (hasDocValues) {
            final Query queryDocValues = new CartesianShapeDocValuesQuery(fieldName, relation.getLuceneRelation(), luceneGeometries);
            query = new IndexOrDocValuesQuery(query, queryDocValues);
        }
        return query;
    }

    private void validateIsShapeFieldType(String fieldName, SearchExecutionContext context) {
        MappedFieldType fieldType = context.getFieldType(fieldName);
        if (fieldType instanceof ShapeFieldMapper.ShapeFieldType == false) {
            throw new QueryShardException(
                context,
                "Expected " + ShapeFieldMapper.CONTENT_TYPE + " field type for Field [" + fieldName + "] but found " + fieldType.typeName()
            );
        }
    }
}
