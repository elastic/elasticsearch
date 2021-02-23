/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.runtimefields.mapper;

import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.geo.Point;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoShapeUtils;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.index.mapper.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.GeoShapeQueryable;
import org.elasticsearch.index.mapper.RuntimeFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.runtimefields.fielddata.GeoPointScriptFieldData;
import org.elasticsearch.runtimefields.query.GeoPointScriptFieldDistanceFeatureQuery;
import org.elasticsearch.runtimefields.query.GeoPointScriptFieldExistsQuery;
import org.elasticsearch.runtimefields.query.GeoPointScriptFieldGeoShapeQuery;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Supplier;

public final class GeoPointScriptFieldType extends AbstractScriptFieldType<GeoPointFieldScript.LeafFactory> implements GeoShapeQueryable {

    public static final RuntimeFieldType.Parser PARSER = new RuntimeFieldType.Parser((name, parserContext) -> new Builder(name) {
        @Override
        protected AbstractScriptFieldType<?> buildFieldType() {
            if (script.get() == null) {
                return new GeoPointScriptFieldType(name, GeoPointFieldScript.PARSE_FROM_SOURCE, this);
            }
            GeoPointFieldScript.Factory factory = parserContext.scriptService().compile(script.getValue(), GeoPointFieldScript.CONTEXT);
            return new GeoPointScriptFieldType(name, factory, this);
        }
    });

    private GeoPointScriptFieldType(String name, GeoPointFieldScript.Factory scriptFactory, Builder builder) {
        super(name, scriptFactory::newFactory, builder);
    }

    GeoPointScriptFieldType(
        String name,
        GeoPointFieldScript.Factory scriptFactory,
        Script script,
        Map<String, String> meta,
        CheckedBiConsumer<XContentBuilder, Boolean, IOException> toXContent
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
