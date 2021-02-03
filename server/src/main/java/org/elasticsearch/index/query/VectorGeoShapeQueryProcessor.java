/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.document.LatLonShape;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.elasticsearch.common.geo.GeoShapeUtils;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.MultiLine;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class VectorGeoShapeQueryProcessor {

    private static final List<Class<? extends Geometry>> WITHIN_UNSUPPORTED_GEOMETRIES = new ArrayList<>();
    static {
        WITHIN_UNSUPPORTED_GEOMETRIES.add(Line.class);
        WITHIN_UNSUPPORTED_GEOMETRIES.add(MultiLine.class);
    }

    public Query geoShapeQuery(Geometry shape, String fieldName, ShapeRelation relation, QueryShardContext context) {
        // CONTAINS queries are not supported by VECTOR strategy for indices created before version 7.5.0 (Lucene 8.3.0)
        if (relation == ShapeRelation.CONTAINS && context.indexVersionCreated().before(Version.V_7_5_0)) {
            throw new QueryShardException(context,
                ShapeRelation.CONTAINS + " query relation not supported for Field [" + fieldName + "].");
        }
        // wrap geoQuery as a ConstantScoreQuery
        return getVectorQueryFromShape(shape, fieldName, relation, context);
    }

    private Query getVectorQueryFromShape(Geometry queryShape, String fieldName, ShapeRelation relation, QueryShardContext context) {
        final LatLonGeometry[] luceneGeometries;
        if (relation == ShapeRelation.WITHIN) {
            luceneGeometries = GeoShapeUtils.toLuceneGeometry(fieldName, context, queryShape, WITHIN_UNSUPPORTED_GEOMETRIES);
        } else {
            luceneGeometries = GeoShapeUtils.toLuceneGeometry(fieldName, context, queryShape, Collections.emptyList());
        }
        if (luceneGeometries.length == 0) {
            return new MatchNoDocsQuery();
        }
        return LatLonShape.newGeometryQuery(fieldName, relation.getLuceneRelation(), luceneGeometries);
    }
}

