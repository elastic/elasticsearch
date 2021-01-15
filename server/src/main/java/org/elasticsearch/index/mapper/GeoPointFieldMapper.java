/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.index.mapper;

import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.AbstractLatLonPointIndexFieldData;
import org.elasticsearch.index.mapper.GeoPointFieldMapper.ParsedGeoPoint;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.VectorGeoPointShapeQueryProcessor;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Field Mapper for geo_point types.
 *
 * Uses lucene 6 LatLonPoint encoding
 */
public class GeoPointFieldMapper extends AbstractPointGeometryFieldMapper<List<ParsedGeoPoint>, List<? extends GeoPoint>> {

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
        final Parameter<Map<String, String>> meta = Parameter.metaParam();

        public Builder(String name, boolean ignoreMalformedByDefault) {
            super(name);
            this.ignoreMalformed = ignoreMalformedParam(m -> builder(m).ignoreMalformed.get(), ignoreMalformedByDefault);
            this.nullValue = nullValueParam(
                m -> builder(m).nullValue.get(),
                (n, c, o) -> parseNullValue(o, ignoreZValue.get().value(), ignoreMalformed.get().value()),
                () -> null).acceptsNull();
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return Arrays.asList(hasDocValues, indexed, stored, ignoreMalformed, ignoreZValue, nullValue, meta);
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

        @Override
        public FieldMapper build(ContentPath contentPath) {
            Parser<List<ParsedGeoPoint>> geoParser = new PointParser<>(name, ParsedGeoPoint::new, (parser, point) -> {
                GeoUtils.parseGeoPoint(parser, point, ignoreZValue.get().value());
                return point;
            }, (ParsedGeoPoint) nullValue.get(), ignoreZValue.get().value(), ignoreMalformed.get().value());
            GeoPointFieldType ft
                = new GeoPointFieldType(buildFullName(contentPath), indexed.get(), stored.get(), hasDocValues.get(), geoParser, meta.get());
            return new GeoPointFieldMapper(name, ft, multiFieldsBuilder.build(this, contentPath),
                copyTo.build(), new GeoPointIndexer(ft), geoParser, this);
        }

    }

    public static TypeParser PARSER = new TypeParser((n, c) -> new Builder(n, IGNORE_MALFORMED_SETTING.get(c.getSettings())));

    private final Builder builder;

    public GeoPointFieldMapper(String simpleName, MappedFieldType mappedFieldType,
                               MultiFields multiFields, CopyTo copyTo,
                               Indexer<List<ParsedGeoPoint>, List<? extends GeoPoint>> indexer,
                               Parser<List<ParsedGeoPoint>> parser,
                               Builder builder) {
        super(simpleName, mappedFieldType, multiFields,
            builder.ignoreMalformed.get(), builder.ignoreZValue.get(), builder.nullValue.get(),
            copyTo, indexer, parser);
        this.builder = builder;
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName(), builder.ignoreMalformed.getDefaultValue().value()).init(this);
    }

    @Override
    protected void addStoredFields(ParseContext context, List<? extends GeoPoint> points) {
        for (GeoPoint point : points) {
            context.doc().add(new StoredField(fieldType().name(), point.toString()));
        }
    }

    @Override
    protected void addMultiFields(ParseContext context, List<? extends GeoPoint> points) throws IOException {
        // @todo phase out geohash (which is currently used in the CompletionSuggester)
        if (points.isEmpty()) {
            return;
        }

        StringBuilder s = new StringBuilder();
        if (points.size() > 1) {
            s.append('[');
        }
        s.append(points.get(0).geohash());
        for (int i = 1; i < points.size(); ++i) {
            s.append(',');
            s.append(points.get(i).geohash());
        }
        if (points.size() > 1) {
            s.append(']');
        }
        multiFields.parse(this, context.createExternalValueContext(s));
    }

    @Override
    protected void addDocValuesFields(String name, List<? extends GeoPoint> points, List<IndexableField> fields, ParseContext context) {
        for (GeoPoint point : points) {
            context.doc().add(new LatLonDocValuesField(fieldType().name(), point.lat(), point.lon()));
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    public static class GeoPointFieldType extends AbstractGeometryFieldType implements GeoShapeQueryable {

        private final VectorGeoPointShapeQueryProcessor queryProcessor;

        private GeoPointFieldType(String name, boolean indexed, boolean stored, boolean hasDocValues,
                                  Parser<?> parser, Map<String, String> meta) {
            super(name, indexed, stored, hasDocValues, true, parser, meta);
            this.queryProcessor = new VectorGeoPointShapeQueryProcessor();
        }

        public GeoPointFieldType(String name) {
            this(name, true, false, true, null, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query geoShapeQuery(Geometry shape, String fieldName, ShapeRelation relation, SearchExecutionContext context) {
            return queryProcessor.geoShapeQuery(shape, fieldName, relation, context);
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

    protected static class GeoPointIndexer implements Indexer<List<ParsedGeoPoint>, List<? extends GeoPoint>> {

        protected final GeoPointFieldType fieldType;

        GeoPointIndexer(GeoPointFieldType fieldType) {
            this.fieldType = fieldType;
        }

        @Override
        public List<? extends GeoPoint> prepareForIndexing(List<ParsedGeoPoint> geoPoints) {
            if (geoPoints == null || geoPoints.isEmpty()) {
                return Collections.emptyList();
            }
            return geoPoints;
        }

        @Override
        public Class<List<? extends GeoPoint>> processedClass() {
            return (Class<List<? extends GeoPoint>>)(Object)List.class;
        }

        @Override
        public List<IndexableField> indexShape(ParseContext context, List<? extends GeoPoint> points) {
            ArrayList<IndexableField> fields = new ArrayList<>(points.size());
            for (GeoPoint point : points) {
                fields.add(new LatLonPoint(fieldType.name(), point.lat(), point.lon()));
            }
            return fields;
        }
    }
}
