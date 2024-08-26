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
import org.elasticsearch.lucene.spatial.CartesianShapeDocValuesQuery;

public class ShapeQueryProcessor {

    public Query shapeQuery(Geometry geometry, String fieldName, ShapeRelation relation, boolean indexed, boolean hasDocValues) {
        assert indexed || hasDocValues;
        if (geometry == null || geometry.isEmpty()) {
            return new MatchNoDocsQuery();
        }
        final XYGeometry[] luceneGeometries = LuceneGeometriesUtils.toXYGeometry(geometry, t -> {});
        Query query;
        if (indexed) {
            query = XYShape.newGeometryQuery(fieldName, relation.getLuceneRelation(), luceneGeometries);
            if (hasDocValues) {
                final Query queryDocValues = new CartesianShapeDocValuesQuery(fieldName, relation.getLuceneRelation(), luceneGeometries);
                query = new IndexOrDocValuesQuery(query, queryDocValues);
            }
        } else {
            query = new CartesianShapeDocValuesQuery(fieldName, relation.getLuceneRelation(), luceneGeometries);
        }
        return query;
    }
}
