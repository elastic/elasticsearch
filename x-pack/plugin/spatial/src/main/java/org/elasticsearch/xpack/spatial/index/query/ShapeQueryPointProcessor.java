/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.index.query;

import org.apache.lucene.document.XYDocValuesField;
import org.apache.lucene.document.XYPointField;
import org.apache.lucene.geo.XYGeometry;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.geo.LuceneGeometriesUtils;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.ShapeType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xpack.spatial.index.mapper.PointFieldMapper;

import java.util.function.Consumer;

public class ShapeQueryPointProcessor {

    public Query shapeQuery(Geometry geometry, String fieldName, ShapeRelation relation, SearchExecutionContext context) {
        final boolean hasDocValues = validateIsPointFieldType(fieldName, context);
        // only the intersects relation is supported for indexed cartesian point types
        if (relation != ShapeRelation.INTERSECTS) {
            throw new QueryShardException(context, relation + " query relation not supported for Field [" + fieldName + "].");
        }
        final Consumer<ShapeType> checker = t -> {
            if (t == ShapeType.POINT || t == ShapeType.MULTIPOINT || t == ShapeType.LINESTRING || t == ShapeType.MULTILINESTRING) {
                throw new QueryShardException(context, "Field [" + fieldName + "] does not support " + t + " queries");
            }
        };
        final XYGeometry[] luceneGeometries = LuceneGeometriesUtils.toXYGeometry(geometry, checker);
        Query query = XYPointField.newGeometryQuery(fieldName, luceneGeometries);
        if (hasDocValues) {
            final Query queryDocValues = XYDocValuesField.newSlowGeometryQuery(fieldName, luceneGeometries);
            query = new IndexOrDocValuesQuery(query, queryDocValues);
        }
        return query;
    }

    private boolean validateIsPointFieldType(String fieldName, SearchExecutionContext context) {
        MappedFieldType fieldType = context.getFieldType(fieldName);
        if (fieldType instanceof PointFieldMapper.PointFieldType == false) {
            throw new QueryShardException(
                context,
                "Expected " + PointFieldMapper.CONTENT_TYPE + " field type for Field [" + fieldName + "] but found " + fieldType.typeName()
            );
        }
        return fieldType.hasDocValues();
    }
}
