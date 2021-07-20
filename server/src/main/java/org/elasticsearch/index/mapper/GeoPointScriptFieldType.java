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
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.index.fielddata.GeoPointScriptFieldData;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.GeoPointFieldScript;
import org.elasticsearch.script.CompositeFieldScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.runtime.GeoPointScriptFieldDistanceFeatureQuery;
import org.elasticsearch.search.runtime.GeoPointScriptFieldExistsQuery;
import org.elasticsearch.search.runtime.GeoPointScriptFieldGeoShapeQuery;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

public final class GeoPointScriptFieldType extends AbstractScriptFieldType<GeoPointFieldScript.LeafFactory> implements GeoShapeQueryable {

    public static final RuntimeField.Parser PARSER = new RuntimeField.Parser(name ->
        new Builder<>(name, GeoPointFieldScript.CONTEXT) {
            @Override
            AbstractScriptFieldType<?> createFieldType(String name,
                                                       GeoPointFieldScript.Factory factory,
                                                       Script script,
                                                       Map<String, String> meta) {
                return new GeoPointScriptFieldType(name, factory, getScript(), meta());
            }

            @Override
            GeoPointFieldScript.Factory getParseFromSourceFactory() {
                return GeoPointFieldScript.PARSE_FROM_SOURCE;
            }

            @Override
            GeoPointFieldScript.Factory getCompositeLeafFactory(
                Function<SearchLookup, CompositeFieldScript.LeafFactory> parentScriptFactory) {
                return GeoPointFieldScript.leafAdapter(parentScriptFactory);
            }
        });

    GeoPointScriptFieldType(
        String name,
        GeoPointFieldScript.Factory scriptFactory,
        Script script,
        Map<String, String> meta
    ) {
        super(name, searchLookup -> scriptFactory.newFactory(name, script.getParams(), searchLookup), script, meta);
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
