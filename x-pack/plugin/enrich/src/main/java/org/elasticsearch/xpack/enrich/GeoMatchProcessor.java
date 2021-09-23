/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.geo.GeometryParser;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.index.query.GeoShapeQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.script.TemplateScript;

import java.util.function.BiConsumer;

public final class GeoMatchProcessor extends AbstractEnrichProcessor {

    private final ShapeRelation shapeRelation;
    private final GeometryParser parser;

    GeoMatchProcessor(
        String tag,
        String description,
        BiConsumer<SearchRequest, BiConsumer<SearchResponse, Exception>> searchRunner,
        String policyName,
        TemplateScript.Factory field,
        TemplateScript.Factory targetField,
        boolean overrideEnabled,
        boolean ignoreMissing,
        String matchField,
        int maxMatches,
        ShapeRelation shapeRelation,
        Orientation orientation
    ) {
        super(tag, description, searchRunner, policyName, field, targetField, ignoreMissing, overrideEnabled, matchField, maxMatches);
        this.shapeRelation = shapeRelation;
        parser = new GeometryParser(orientation.getAsBoolean(), true, true);
    }

    @Override
    public QueryBuilder getQueryBuilder(Object fieldValue) {
        final Geometry queryGeometry = parser.parseGeometry(fieldValue);
        GeoShapeQueryBuilder shapeQuery = new GeoShapeQueryBuilder(matchField, queryGeometry);
        shapeQuery.relation(shapeRelation);
        return shapeQuery;
    }

    public ShapeRelation getShapeRelation() {
        return shapeRelation;
    }
}
