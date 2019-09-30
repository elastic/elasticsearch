/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.index.query.GeoShapeQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.function.BiConsumer;

public final class GeoMatchProcessor extends AbstractEnrichProcessor {

    private ShapeRelation shapeRelation;

    GeoMatchProcessor(String tag,
                      Client client,
                      String policyName,
                      String field,
                      String targetField,
                      boolean overrideEnabled,
                      boolean ignoreMissing,
                      String matchField,
                      int maxMatches,
                      ShapeRelation shapeRelation) {
        super(tag, client, policyName, field, targetField, ignoreMissing, overrideEnabled, matchField, maxMatches);
        this.shapeRelation = shapeRelation;
    }

    /** used in tests **/
    GeoMatchProcessor(String tag,
                      BiConsumer<SearchRequest, BiConsumer<SearchResponse, Exception>> searchRunner,
                      String policyName,
                      String field,
                      String targetField,
                      boolean overrideEnabled,
                      boolean ignoreMissing,
                      String matchField,
                      int maxMatches, ShapeRelation shapeRelation) {
        super(tag, searchRunner, policyName, field, targetField, ignoreMissing, overrideEnabled, matchField, maxMatches);
        this.shapeRelation = shapeRelation;
    }

    @Override
    public QueryBuilder getQueryBuilder(Object fieldValue) {
        GeoPoint point = GeoUtils.parseGeoPoint(fieldValue, true);
        GeoShapeQueryBuilder shapeQuery = new GeoShapeQueryBuilder(matchField, new Point(point.lon(), point.lat()));
        shapeQuery.relation(shapeRelation);
        return shapeQuery;
    }

    public ShapeRelation getShapeRelation() {
        return shapeRelation;
    }
}
