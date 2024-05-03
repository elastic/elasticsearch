/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.mapper;

import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.geo.GeoFormatterFactory;
import org.elasticsearch.common.geo.GeometryFormatterFactory;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.mapper.AbstractScriptFieldType;
import org.elasticsearch.index.mapper.GeoShapeQueryable;
import org.elasticsearch.index.mapper.OnScriptError;
import org.elasticsearch.index.mapper.RuntimeField;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.CompositeFieldScript;
import org.elasticsearch.script.GeometryFieldScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.xpack.spatial.index.fielddata.plain.GeoShapeScriptFieldData;
import org.elasticsearch.xpack.spatial.search.runtime.GeoShapeScriptFieldExistsQuery;
import org.elasticsearch.xpack.spatial.search.runtime.GeoShapeScriptFieldGeoShapeQuery;

import java.io.IOException;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public final class GeoShapeScriptFieldType extends AbstractScriptFieldType<GeometryFieldScript.LeafFactory> implements GeoShapeQueryable {

    public static RuntimeField.Parser typeParser(GeoFormatterFactory<Geometry> geoFormatterFactory) {
        return new RuntimeField.Parser(name -> new Builder<>(name, GeometryFieldScript.CONTEXT) {
            @Override
            protected AbstractScriptFieldType<?> createFieldType(
                String name,
                GeometryFieldScript.Factory factory,
                Script script,
                Map<String, String> meta,
                OnScriptError onScriptError
            ) {
                return new GeoShapeScriptFieldType(name, factory, getScript(), meta(), onScriptError, geoFormatterFactory);
            }

            @Override
            protected GeometryFieldScript.Factory getParseFromSourceFactory() {
                return GeometryFieldScript.PARSE_FROM_SOURCE;
            }

            @Override
            protected GeometryFieldScript.Factory getCompositeLeafFactory(
                Function<SearchLookup, CompositeFieldScript.LeafFactory> parentScriptFactory
            ) {
                return GeometryFieldScript.leafAdapter(parentScriptFactory);
            }
        });
    }

    private final GeoFormatterFactory<Geometry> geoFormatterFactory;

    GeoShapeScriptFieldType(
        String name,
        GeometryFieldScript.Factory scriptFactory,
        Script script,
        Map<String, String> meta,
        OnScriptError onScriptError,
        GeoFormatterFactory<Geometry> geoFormatterFactory
    ) {
        super(
            name,
            searchLookup -> scriptFactory.newFactory(name, script.getParams(), searchLookup, onScriptError),
            script,
            scriptFactory.isResultDeterministic(),
            meta
        );
        this.geoFormatterFactory = geoFormatterFactory;
    }

    @Override
    public String typeName() {
        return GeoShapeWithDocValuesFieldMapper.CONTENT_TYPE;
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
    public GeoShapeScriptFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
        return new GeoShapeScriptFieldData.Builder(
            name(),
            leafFactory(fieldDataContext.lookupSupplier().get()),
            GeoShapeWithDocValuesFieldMapper.GeoShapeDocValuesField::new
        );
    }

    @Override
    public Query existsQuery(SearchExecutionContext context) {
        applyScriptContext(context);
        return new GeoShapeScriptFieldExistsQuery(script, leafFactory(context), name());
    }

    @Override
    public Query geoShapeQuery(SearchExecutionContext context, String fieldName, ShapeRelation relation, LatLonGeometry... geometries) {
        return new GeoShapeScriptFieldGeoShapeQuery(script, leafFactory(context), fieldName, relation, geometries);
    }

    @Override
    public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
        GeometryFieldScript.LeafFactory leafFactory = leafFactory(context.lookup());

        Function<List<Geometry>, List<Object>> formatter = geoFormatterFactory.getFormatter(
            format != null ? format : GeometryFormatterFactory.GEOJSON,
            Function.identity()
        );
        return new ValueFetcher() {
            private GeometryFieldScript script;

            @Override
            public void setNextReader(LeafReaderContext context) {
                script = leafFactory.newInstance(context);
            }

            @Override
            public List<Object> fetchValues(Source source, int doc, List<Object> ignoredValues) throws IOException {
                script.runForDoc(doc);
                if (script.count() == 0) {
                    return List.of();
                }
                return formatter.apply(List.of(script.geometry()));
            }

            @Override
            public StoredFieldsSpec storedFieldsSpec() {
                return StoredFieldsSpec.NEEDS_SOURCE;
            }
        };
    }
}
