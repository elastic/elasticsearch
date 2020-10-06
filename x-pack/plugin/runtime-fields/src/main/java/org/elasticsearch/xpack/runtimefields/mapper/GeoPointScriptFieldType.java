/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.mapper;

import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.geo.GeoShapeUtils;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.MultiLine;
import org.elasticsearch.index.mapper.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.GeoShapeQueryable;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.xpack.runtimefields.fielddata.GeoPointScriptFieldData;
import org.elasticsearch.xpack.runtimefields.query.GeoPointScriptFieldExistsQuery;
import org.elasticsearch.xpack.runtimefields.query.GeoPointScriptFieldGeoShapeQuery;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public final class GeoPointScriptFieldType extends AbstractScriptFieldType<GeoPointFieldScript.LeafFactory> implements GeoShapeQueryable {

    private static final List<Class<? extends Geometry>> UNSUPPORTED_GEOMETRIES = new ArrayList<>();
    static {
        UNSUPPORTED_GEOMETRIES.add(Line.class);
        UNSUPPORTED_GEOMETRIES.add(MultiLine.class);
    }

    GeoPointScriptFieldType(String name, Script script, GeoPointFieldScript.Factory scriptFactory, Map<String, String> meta) {
        super(name, script, scriptFactory::newFactory, meta);
    }

    @Override
    protected String runtimeType() {
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
        QueryShardContext context
    ) {
        throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] does not support range queries");
    }

    @Override
    public Query termQuery(Object value, QueryShardContext context) {
        throw new IllegalArgumentException(
            "Geometry fields do not support exact searching, use dedicated geometry queries instead: [" + name() + "]"
        );
    }

    @Override
    public GeoPointScriptFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
        return new GeoPointScriptFieldData.Builder(name(), leafFactory(searchLookup.get()));
    }

    @Override
    public Query existsQuery(QueryShardContext context) {
        checkAllowExpensiveQueries(context);
        return new GeoPointScriptFieldExistsQuery(script, leafFactory(context), name());
    }

    @Override
    public Query geoShapeQuery(Geometry shape, String fieldName, ShapeRelation relation, QueryShardContext context) {
        if (shape == null) {
            return new MatchNoDocsQuery();
        }
        final LatLonGeometry[] geometries = GeoShapeUtils.toLuceneGeometry(fieldName, context, shape, UNSUPPORTED_GEOMETRIES);
        return new GeoPointScriptFieldGeoShapeQuery(script, leafFactory(context), fieldName, geometries);
    }
}
