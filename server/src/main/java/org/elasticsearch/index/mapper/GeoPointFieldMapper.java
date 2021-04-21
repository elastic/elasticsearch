/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.mapper;

import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.geo.GeoJsonGeometryFormat;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoShapeUtils;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.geo.GeometryFormat;
import org.elasticsearch.common.geo.GeometryParser;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.AbstractLatLonPointIndexFieldData;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.GeoPointFieldScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptCompiler;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.lookup.FieldValues;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Field Mapper for geo_point types.
 *
 * Uses lucene 6 LatLonPoint encoding
 */
public class GeoPointFieldMapper extends AbstractPointGeometryFieldMapper<GeoPoint> {

    public static final String CONTENT_TYPE = "geo_point";

    private static Builder builder(FieldMapper in) {
        return ((GeoPointFieldMapper)in).builder;
    }

    public static class Builder extends FieldMapper.Builder {

        final Parameter<Explicit<Boolean>> ignoreMalformed;
        final Parameter<Explicit<Boolean>> ignoreZValue = ignoreZValueParam(m -> builder(m).ignoreZValue.get());
        final Parameter<ParsedPoint> nullValue;
        final Parameter<Boolean> indexed = Parameter.indexParam(m -> builder(m).indexed.get(), true);
        final Parameter<Boolean> hasDocValues = Parameter.docValuesParam(m -> builder(m).hasDocValues.get(), true);
        final Parameter<Boolean> stored = Parameter.storeParam(m -> builder(m).stored.get(), false);
        private final Parameter<Script> script = Parameter.scriptParam(m -> builder(m).script.get());
        private final Parameter<String> onScriptError = Parameter.onScriptErrorParam(m -> builder(m).onScriptError.get(), script);
        final Parameter<Map<String, String>> meta = Parameter.metaParam();

        private final ScriptCompiler scriptCompiler;

        public Builder(String name, ScriptCompiler scriptCompiler, boolean ignoreMalformedByDefault) {
            super(name);
            this.ignoreMalformed = ignoreMalformedParam(m -> builder(m).ignoreMalformed.get(), ignoreMalformedByDefault);
            this.nullValue = nullValueParam(
                m -> builder(m).nullValue.get(),
                (n, c, o) -> parseNullValue(o, ignoreZValue.get().value(), ignoreMalformed.get().value()),
                () -> null).acceptsNull();
            this.scriptCompiler = Objects.requireNonNull(scriptCompiler);
            this.script.precludesParameters(nullValue, ignoreMalformed, ignoreZValue);
            addScriptValidation(script, indexed, hasDocValues);
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return Arrays.asList(hasDocValues, indexed, stored, ignoreMalformed, ignoreZValue, nullValue, script, onScriptError, meta);
        }

        public Builder docValues(boolean hasDocValues) {
            this.hasDocValues.setValue(hasDocValues);
            return this;
        }

        private static ParsedGeoPoint parseNullValue(Object nullValue, boolean ignoreZValue, boolean ignoreMalformed) {
            if (nullValue == null) {
                return null;
            }
            ParsedGeoPoint point = new ParsedGeoPoint();
            GeoUtils.parseGeoPoint(nullValue, point, ignoreZValue);
            if (ignoreMalformed == false) {
                if (point.lat() > 90.0 || point.lat() < -90.0) {
                    throw new IllegalArgumentException("illegal latitude value [" + point.lat() + "]");
                }
                if (point.lon() > 180.0 || point.lon() < -180) {
                    throw new IllegalArgumentException("illegal longitude value [" + point.lon() + "]");
                }
            } else {
                GeoUtils.normalizePoint(point);
            }
            return point;
        }

        private FieldValues<GeoPoint> scriptValues() {
            if (this.script.get() == null) {
                return null;
            }
            GeoPointFieldScript.Factory factory = scriptCompiler.compile(this.script.get(), GeoPointFieldScript.CONTEXT);
            return factory == null ? null : (lookup, ctx, doc, consumer) -> factory
                .newFactory(name, script.get().getParams(), lookup)
                .newInstance(ctx)
                .runGeoPointForDoc(doc, consumer);
        }

        @Override
        public FieldMapper build(ContentPath contentPath) {
            Parser<ParsedGeoPoint> geoParser = new PointParser<>(
                name,
                ParsedGeoPoint::new,
                (parser, point) -> {
                    GeoUtils.parseGeoPoint(parser, point, ignoreZValue.get().value());
                    return point;
                },
                (ParsedGeoPoint) nullValue.get(),
                ignoreZValue.get().value(),
                ignoreMalformed.get().value());
            GeoPointFieldType ft = new GeoPointFieldType(
                buildFullName(contentPath),
                indexed.get(),
                stored.get(),
                hasDocValues.get(),
                geoParser,
                scriptValues(),
                meta.get());
            if (this.script.get() == null) {
                return new GeoPointFieldMapper(name, ft, multiFieldsBuilder.build(this, contentPath),
                    copyTo.build(), geoParser, this);
            }
            return new GeoPointFieldMapper(name, ft, geoParser, this);
        }

    }

    public static TypeParser PARSER
        = new TypeParser((n, c) -> new Builder(n, c.scriptCompiler(), IGNORE_MALFORMED_SETTING.get(c.getSettings())));

    private final Builder builder;
    private final FieldValues<GeoPoint> scriptValues;

    public GeoPointFieldMapper(String simpleName, MappedFieldType mappedFieldType,
                               MultiFields multiFields, CopyTo copyTo,
                               Parser<ParsedGeoPoint> parser,
                               Builder builder) {
        super(simpleName, mappedFieldType, multiFields,
            builder.ignoreMalformed.get(), builder.ignoreZValue.get(), builder.nullValue.get(),
            copyTo, parser);
        this.builder = builder;
        this.scriptValues = null;
    }

    public GeoPointFieldMapper(String simpleName, MappedFieldType mappedFieldType,
                               Parser<ParsedGeoPoint> parser, Builder builder) {
        super(simpleName, mappedFieldType, MultiFields.empty(), CopyTo.empty(), parser, builder.onScriptError.get());
        this.builder = builder;
        this.scriptValues = builder.scriptValues();
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName(), builder.scriptCompiler, builder.ignoreMalformed.getDefaultValue().value()).init(this);
    }

    @Override
    protected void index(ParseContext context, GeoPoint geometry) throws IOException {
        if (fieldType().isSearchable()) {
            context.doc().add(new LatLonPoint(fieldType().name(), geometry.lat(), geometry.lon()));
        }
        if (fieldType().hasDocValues()) {
            context.doc().add(new LatLonDocValuesField(fieldType().name(), geometry.lat(), geometry.lon()));
        } else if (fieldType().isStored() || fieldType().isSearchable()) {
            createFieldNamesField(context);
        }
        if (fieldType().isStored()) {
            context.doc().add(new StoredField(fieldType().name(), geometry.toString()));
        }
        // TODO phase out geohash (which is currently used in the CompletionSuggester)
        multiFields.parse(this, context.createExternalValueContext(geometry.geohash()));
    }

    @Override
    protected void indexScriptValues(SearchLookup searchLookup, LeafReaderContext readerContext, int doc, ParseContext parseContext) {
        this.scriptValues.valuesForDoc(searchLookup, readerContext, doc, point -> {
            try {
                index(parseContext, point);
            } catch (IOException e) {
                throw new UncheckedIOException(e);  // only thrown by MultiFields which is always null
            }
        });
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    public static class GeoPointFieldType extends AbstractGeometryFieldType implements GeoShapeQueryable {

        private static final GeometryParser PARSER = new GeometryParser(true, true, true);
        private final FieldValues<GeoPoint> scriptValues;

        private GeoPointFieldType(String name, boolean indexed, boolean stored, boolean hasDocValues,
                                  Parser<ParsedGeoPoint> parser, FieldValues<GeoPoint> scriptValues, Map<String, String> meta) {
            super(name, indexed, stored, hasDocValues, true, parser, meta);
            this.scriptValues = scriptValues;
        }

        public GeoPointFieldType(String name) {
            this(name, true, false, true, null, null, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            if (scriptValues == null) {
                return super.valueFetcher(context, format);
            }
            String geoFormat = format != null ? format : GeoJsonGeometryFormat.NAME;
            GeometryFormat<Geometry> geometryFormat = PARSER.geometryFormat(geoFormat);
            return FieldValues.valueFetcher(scriptValues, v -> {
                GeoPoint p = (GeoPoint) v;
                return geometryFormat.toXContentAsObject(new Point(p.lon(), p.lat()));
            }, context);
        }

        @Override
        public Query geoShapeQuery(Geometry shape, String fieldName, ShapeRelation relation, SearchExecutionContext context) {
            final LatLonGeometry[] luceneGeometries = GeoShapeUtils.toLuceneGeometry(fieldName, context, shape, relation);
            if (luceneGeometries.length == 0) {
                return new MatchNoDocsQuery();
            }
            Query query = LatLonPoint.newGeometryQuery(fieldName, relation.getLuceneRelation(), luceneGeometries);
            if (hasDocValues()) {
                Query dvQuery = LatLonDocValuesField.newSlowGeometryQuery(fieldName, relation.getLuceneRelation(), luceneGeometries);
                query = new IndexOrDocValuesQuery(query, dvQuery);
            }
            return query;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
            failIfNoDocValues();
            return new AbstractLatLonPointIndexFieldData.Builder(name(), CoreValuesSourceType.GEOPOINT);
        }

        @Override
        public Query distanceFeatureQuery(Object origin, String pivot, SearchExecutionContext context) {
            GeoPoint originGeoPoint;
            if (origin instanceof GeoPoint) {
                originGeoPoint = (GeoPoint) origin;
            } else if (origin instanceof String) {
                originGeoPoint = GeoUtils.parseFromString((String) origin);
            } else {
                throw new IllegalArgumentException("Illegal type ["+ origin.getClass() + "] for [origin]! " +
                    "Must be of type [geo_point] or [string] for geo_point fields!");
            }
            double pivotDouble = DistanceUnit.DEFAULT.parse(pivot, DistanceUnit.DEFAULT);
            // As we already apply boost in AbstractQueryBuilder::toQuery, we always passing a boost of 1.0 to distanceFeatureQuery
            return LatLonPoint.newDistanceFeatureQuery(name(), 1.0f, originGeoPoint.lat(), originGeoPoint.lon(), pivotDouble);
        }
    }

    // Eclipse requires the AbstractPointGeometryFieldMapper prefix or it can't find ParsedPoint
    // See https://bugs.eclipse.org/bugs/show_bug.cgi?id=565255
    protected static class ParsedGeoPoint extends GeoPoint implements AbstractPointGeometryFieldMapper.ParsedPoint {
        @Override
        public void validate(String fieldName) {
            if (lat() > 90.0 || lat() < -90.0) {
                throw new IllegalArgumentException("illegal latitude value [" + lat() + "] for " + fieldName);
            }
            if (lon() > 180.0 || lon() < -180) {
                throw new IllegalArgumentException("illegal longitude value [" + lon() + "] for " + fieldName);
            }
        }

        @Override
        public void normalize(String name) {
            if (isNormalizable(lat()) && isNormalizable(lon())) {
                GeoUtils.normalizePoint(this);
            } else {
                throw new ElasticsearchParseException("cannot normalize the point - not a number");
            }
        }

        @Override
        public boolean isNormalizable(double coord) {
            return Double.isNaN(coord) == false && Double.isInfinite(coord) == false;
        }

        @Override
        public void resetCoords(double x, double y) {
            this.reset(y, x);
        }

        public Point asGeometry() {
            return new Point(lon(), lat());
        }

        @Override
        public boolean equals(Object other) {
            double oLat;
            double oLon;
            if (other instanceof GeoPoint) {
                GeoPoint o = (GeoPoint)other;
                oLat = o.lat();
                oLon = o.lon();
            } else {
                return false;
            }
            if (Double.compare(oLat, lat) != 0) return false;
            if (Double.compare(oLon, lon) != 0) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }
    }

}
