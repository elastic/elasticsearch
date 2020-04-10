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
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.index.query.GeoShapeQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.script.TemplateScript;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

public final class GeoMatchProcessor extends AbstractEnrichProcessor {

    private ShapeRelation shapeRelation;

    GeoMatchProcessor(
        String tag,
        Client client,
        String policyName,
        TemplateScript.Factory field,
        TemplateScript.Factory targetField,
        boolean overrideEnabled,
        boolean ignoreMissing,
        String matchField,
        int maxMatches,
        ShapeRelation shapeRelation
    ) {
        super(tag, client, policyName, field, targetField, ignoreMissing, overrideEnabled, matchField, maxMatches);
        this.shapeRelation = shapeRelation;
    }

    /** used in tests **/
    GeoMatchProcessor(
        String tag,
        BiConsumer<SearchRequest, BiConsumer<SearchResponse, Exception>> searchRunner,
        String policyName,
        TemplateScript.Factory field,
        TemplateScript.Factory targetField,
        boolean overrideEnabled,
        boolean ignoreMissing,
        String matchField,
        int maxMatches,
        ShapeRelation shapeRelation
    ) {
        super(tag, searchRunner, policyName, field, targetField, ignoreMissing, overrideEnabled, matchField, maxMatches);
        this.shapeRelation = shapeRelation;
    }

    @Override
    public QueryBuilder getQueryBuilder(Object fieldValue) {
        List<Point> points = new ArrayList<>();
        if (fieldValue instanceof List) {
            List<?> values = (List<?>) fieldValue;
            if (values.size() == 2 && values.get(0) instanceof Number) {
                GeoPoint geoPoint = GeoUtils.parseGeoPoint(values, true);
                points.add(new Point(geoPoint.lon(), geoPoint.lat()));
            } else {
                for (Object value : values) {
                    GeoPoint geoPoint = GeoUtils.parseGeoPoint(value, true);
                    points.add(new Point(geoPoint.lon(), geoPoint.lat()));
                }
            }
        } else {
            GeoPoint geoPoint = GeoUtils.parseGeoPoint(fieldValue, true);
            points.add(new Point(geoPoint.lon(), geoPoint.lat()));
        }
        final Geometry queryGeometry;
        if (points.isEmpty()) {
            throw new IllegalArgumentException("no geopoints found");
        } else if (points.size() == 1) {
            queryGeometry = points.get(0);
        } else {
            queryGeometry = new MultiPoint(points);
        }
        GeoShapeQueryBuilder shapeQuery = new GeoShapeQueryBuilder(matchField, queryGeometry);
        shapeQuery.relation(shapeRelation);
        return shapeQuery;
    }

    public ShapeRelation getShapeRelation() {
        return shapeRelation;
    }
}
