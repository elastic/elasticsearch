/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.geo.Point;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.geo.GeometryFormatterFactory;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.GeoPointScriptFieldData;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.AbstractLongFieldScript;
import org.elasticsearch.script.CompositeFieldScript;
import org.elasticsearch.script.GeoPointFieldScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.field.GeoPointDocValuesField;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.search.runtime.GeoPointScriptFieldDistanceFeatureQuery;
import org.elasticsearch.search.runtime.GeoPointScriptFieldExistsQuery;
import org.elasticsearch.search.runtime.GeoPointScriptFieldGeoShapeQuery;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.index.mapper.GeoPointFieldMapper.GeoPointFieldType.GEO_FORMATTER_FACTORY;

public final class GeoPointScriptFieldType extends AbstractScriptFieldType<GeoPointFieldScript.LeafFactory> implements GeoShapeQueryable {

    public static final RuntimeField.Parser PARSER = new RuntimeField.Parser(name -> new Builder<>(name, GeoPointFieldScript.CONTEXT) {
        @Override
        protected AbstractScriptFieldType<?> createFieldType(
            String name,
            GeoPointFieldScript.Factory factory,
            Script script,
            Map<String, String> meta,
            OnScriptError onScriptError
        ) {
            return new GeoPointScriptFieldType(name, factory, getScript(), meta(), onScriptError);
        }

        @Override
        protected GeoPointFieldScript.Factory getParseFromSourceFactory() {
            return GeoPointFieldScript.PARSE_FROM_SOURCE;
        }

        @Override
        protected GeoPointFieldScript.Factory getCompositeLeafFactory(
            Function<SearchLookup, CompositeFieldScript.LeafFactory> parentScriptFactory
        ) {
            return GeoPointFieldScript.leafAdapter(parentScriptFactory);
        }
    });

    GeoPointScriptFieldType(
        String name,
        GeoPointFieldScript.Factory scriptFactory,
        Script script,
        Map<String, String> meta,
        OnScriptError onScriptError
    ) {
        super(
            name,
            searchLookup -> scriptFactory.newFactory(name, script.getParams(), searchLookup, onScriptError),
            script,
            scriptFactory.isResultDeterministic(),
            meta,
            scriptFactory.isParsedFromSource()
        );
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
    public GeoPointScriptFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
        return new GeoPointScriptFieldData.Builder(
            name(),
            leafFactory(fieldDataContext.lookupSupplier().get()),
            GeoPointDocValuesField::new
        );
    }

    @Override
    public Query existsQuery(SearchExecutionContext context) {
        applyScriptContext(context);
        return new GeoPointScriptFieldExistsQuery(script, leafFactory(context), name());
    }

    @Override
    public Query geoShapeQuery(SearchExecutionContext context, String fieldName, ShapeRelation relation, LatLonGeometry... geometries) {
        if (relation == ShapeRelation.CONTAINS && Arrays.stream(geometries).anyMatch(g -> (g instanceof Point) == false)) {
            return Queries.NO_DOCS_INSTANCE;
        }
        return new GeoPointScriptFieldGeoShapeQuery(script, leafFactory(context), fieldName, relation, geometries);
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
            valuesEncodedAsLong(context.lookup(), name(), leafFactory(context)::newInstance),
            name(),
            originGeoPoint.lat(),
            originGeoPoint.lon(),
            pivotDouble
        );
    }

    public static Function<LeafReaderContext, AbstractLongFieldScript> valuesEncodedAsLong(
        SearchLookup lookup,
        String name,
        Function<LeafReaderContext, GeoPointFieldScript> delegateLeafFactory
    ) {
        return ctx -> {
            GeoPointFieldScript script = delegateLeafFactory.apply(ctx);
            return new AbstractLongFieldScript(name, Map.of(), lookup, OnScriptError.FAIL, ctx) {
                private int docId;

                @Override
                protected void emitFromObject(Object v) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public void setDocument(int docID) {
                    super.setDocument(docID);
                    this.docId = docID;
                }

                @Override
                public void execute() {
                    script.runForDoc(docId);
                    for (int i = 0; i < script.count(); i++) {
                        int latitudeEncoded = GeoEncodingUtils.encodeLatitude(script.lats()[i]);
                        int longitudeEncoded = GeoEncodingUtils.encodeLongitude(script.lons()[i]);
                        emit((((long) latitudeEncoded) << 32) | (longitudeEncoded & 0xFFFFFFFFL));
                    }
                }
            };
        };
    }

    @Override
    public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
        GeoPointFieldScript.LeafFactory leafFactory = leafFactory(context.lookup());
        Function<List<GeoPoint>, List<Object>> formatter = GeoPointFieldMapper.GeoPointFieldType.GEO_FORMATTER_FACTORY.getFormatter(
            format != null ? format : GeometryFormatterFactory.GEOJSON,
            p -> new org.elasticsearch.geometry.Point(p.getLon(), p.getLat())
        );
        return new ValueFetcher() {
            private GeoPointFieldScript script;

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
                List<GeoPoint> points = new ArrayList<>(script.count());
                for (int i = 0; i < script.count(); i++) {
                    points.add(new GeoPoint(script.lats()[i], script.lons()[i]));
                }
                return formatter.apply(points);
            }

            @Override
            public StoredFieldsSpec storedFieldsSpec() {
                return StoredFieldsSpec.NEEDS_SOURCE;
            }
        };
    }

    @Override
    public BlockLoader blockLoader(BlockLoaderContext blContext) {
        FallbackSyntheticSourceBlockLoader fallbackSyntheticSourceBlockLoader = fallbackSyntheticSourceBlockLoader(
            blContext,
            BlockLoader.BlockFactory::bytesRefs,
            this::fallbackSyntheticSourceBlockLoaderReader
        );

        if (fallbackSyntheticSourceBlockLoader != null) {
            return fallbackSyntheticSourceBlockLoader;
        }

        // the rest is not yet supported, so call super
        return super.blockLoader(blContext);
    }

    private FallbackSyntheticSourceBlockLoader.Reader<?> fallbackSyntheticSourceBlockLoaderReader() {
        // parses Objects into GeoPoints
        AbstractGeometryFieldMapper.Parser<GeoPoint> geoPointParser = createGeoPointParser();

        // WKB is how what we store geometries as in Lucene, this also matches the format defined in
        // AbstractGeometryFieldMapper.AbstractGeometryFieldType.GeometriesFallbackSyntheticSourceReader, which GeoPointFieldMapper uses
        var formatter = GEO_FORMATTER_FACTORY.getFormatter(
            GeometryFormatterFactory.WKB,
            p -> new org.elasticsearch.geometry.Point(p.getLon(), p.getLat())
        );

        return new FallbackSyntheticSourceBlockLoader.Reader<GeoPoint>() {
            @Override
            public void convertValue(Object value, List<GeoPoint> accumulator) {
                geoPointParser.fetchFromSource(value, gp -> {
                    if (gp != null) {
                        accumulator.add(gp);
                    }
                });
            }

            @Override
            public void parse(XContentParser parser, List<GeoPoint> accumulator) throws IOException {
                // geo objects can be defined in many ways, and these are more complex compared to basic types like text or number, as a
                // result we must use a dedicated parser here
                geoPointParser.parseFromSource(parser, gp -> {
                    if (gp != null) {
                        accumulator.add(gp);
                    }
                });
            }

            @Override
            public void writeToBlock(List<GeoPoint> points, BlockLoader.Builder blockBuilder) {
                var bytesRefBuilder = (BlockLoader.BytesRefBuilder) blockBuilder;
                List<Object> gpBytes = formatter.apply(points);

                for (var bytes : gpBytes) {
                    // WKB is a binary storage format for geometric data
                    if (bytes instanceof byte[] wkb) {
                        bytesRefBuilder.appendBytesRef(new BytesRef(wkb));
                    }
                }
            }
        };
    }

    private AbstractGeometryFieldMapper.Parser<GeoPoint> createGeoPointParser() {
        // only needed during indexing, which is not done for runtime fields
        boolean ignoredMalformedEnabled = false;
        boolean storeMalformedDataForSyntheticSource = false;

        // we don't know how we stored things, so we need to enable this otherwise parsing can fail
        boolean allowMultipleValues = true;
        boolean ignoreZValue = true;

        GeoPoint nullValue = null;

        return new GeoPointFieldMapper.GeoPointParser(
            name(),
            (parser) -> GeoUtils.parseGeoPoint(parser, ignoreZValue),
            nullValue,
            ignoreZValue,
            ignoredMalformedEnabled,
            allowMultipleValues,
            storeMalformedDataForSyntheticSource
        );
    }

}
