/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.geo.Point;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoShapeUtils;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.index.fielddata.GeoPointScriptFieldData;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.GeoPointFieldScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.runtime.GeoPointScriptFieldDistanceFeatureQuery;
import org.elasticsearch.search.runtime.GeoPointScriptFieldExistsQuery;
import org.elasticsearch.search.runtime.GeoPointScriptFieldGeoShapeQuery;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public final class GeoPointScriptFieldType extends AbstractScriptFieldType<GeoPointFieldScript.LeafFactory> implements GeoShapeQueryable {

    private static final GeoPointFieldScript.Factory PARSE_FROM_SOURCE
        = (field, params, lookup) -> (GeoPointFieldScript.LeafFactory) ctx -> new GeoPointFieldScript
        (
            field,
            params,
            lookup,
            ctx
        ) {
        private final GeoPoint scratch = new GeoPoint();

        @Override
        public void execute() {
            try {
                Object value = XContentMapValues.extractValue(field, leafSearchLookup.source().source());
                if (value instanceof List<?>) {
                    List<?> values = (List<?>) value;
                    if (values.size() > 0 && values.get(0) instanceof Number) {
                        parsePoint(value);
                    } else {
                        for (Object point : values) {
                            parsePoint(point);
                        }
                    }
                } else {
                    parsePoint(value);
                }
            } catch (Exception e) {
                // ignore
            }
        }

        private void parsePoint(Object point) {
            if (point != null) {
                GeoUtils.parseGeoPoint(point, scratch, true);
                emit(scratch.lat(), scratch.lon());
            }
        }
    };

    public static final RuntimeField.Parser PARSER = new RuntimeField.Parser(name ->
        new Builder<>(name, GeoPointFieldScript.CONTEXT, PARSE_FROM_SOURCE) {
            @Override
            RuntimeField newRuntimeField(GeoPointFieldScript.Factory scriptFactory) {
                return new GeoPointScriptFieldType(name, scriptFactory, getScript(), meta(), this);
            }
        });

    GeoPointScriptFieldType(
        String name,
        GeoPointFieldScript.Factory scriptFactory,
        Script script,
        Map<String, String> meta,
        ToXContent toXContent
    ) {
        super(name, scriptFactory::newFactory, script, meta, toXContent);
    }

    @Override
    public String typeName() {
        return GeoPointFieldMapper.CONTENT_TYPE;
    }

    @Override
    protected Query rangeQuery(
        Object lowerTerm,
        Object upperTerm,
        boolean includeLower,
        boolean includeUpper,
        ZoneId timeZone,
        DateMathParser parser,
        SearchExecutionContext context
    ) {
        throw new IllegalArgumentException("Runtime field [" + name() + "] of type [" + typeName() + "] does not support range queries");
    }

    @Override
    public Query termQuery(Object value, SearchExecutionContext context) {
        throw new IllegalArgumentException(
            "Geometry fields do not support exact searching, use dedicated geometry queries instead: [" + name() + "]"
        );
    }

    @Override
    public GeoPointScriptFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
        return new GeoPointScriptFieldData.Builder(name(), leafFactory(searchLookup.get()));
    }

    @Override
    public Query existsQuery(SearchExecutionContext context) {
        checkAllowExpensiveQueries(context);
        return new GeoPointScriptFieldExistsQuery(script, leafFactory(context), name());
    }

    @Override
    public Query geoShapeQuery(Geometry shape, String fieldName, ShapeRelation relation, SearchExecutionContext context) {
        final LatLonGeometry[] luceneGeometries = GeoShapeUtils.toLuceneGeometry(fieldName, context, shape, relation);
        if (luceneGeometries.length == 0
            || (relation == ShapeRelation.CONTAINS && Arrays.stream(luceneGeometries).anyMatch(g -> (g instanceof Point) == false))) {
            return new MatchNoDocsQuery();
        }
        return new GeoPointScriptFieldGeoShapeQuery(script, leafFactory(context), fieldName, relation, luceneGeometries);
    }

    @Override
    public Query distanceFeatureQuery(Object origin, String pivot, SearchExecutionContext context) {
        GeoPoint originGeoPoint;
        if (origin instanceof GeoPoint) {
            originGeoPoint = (GeoPoint) origin;
        } else if (origin instanceof String) {
            originGeoPoint = GeoUtils.parseFromString((String) origin);
        } else {
            throw new IllegalArgumentException(
                "Illegal type [" + origin.getClass() + "] for [origin]! " + "Must be of type [geo_point] or [string] for geo_point fields!"
            );
        }
        double pivotDouble = DistanceUnit.DEFAULT.parse(pivot, DistanceUnit.DEFAULT);
        return new GeoPointScriptFieldDistanceFeatureQuery(
            script,
            leafFactory(context)::newInstance,
            name(),
            originGeoPoint.lat(),
            originGeoPoint.lon(),
            pivotDouble
        );
    }
}
