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

import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.AbstractLatLonPointIndexFieldData;
import org.elasticsearch.index.mapper.GeoPointFieldMapper.ParsedGeoPoint;
import org.elasticsearch.index.query.VectorGeoPointShapeQueryProcessor;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Field Mapper for geo_point types.
 *
 * Uses lucene 6 LatLonPoint encoding
 */
public class GeoPointFieldMapper extends AbstractPointGeometryFieldMapper<List<ParsedGeoPoint>, List<? extends GeoPoint>> {
    public static final String CONTENT_TYPE = "geo_point";
    public static final FieldType FIELD_TYPE = new FieldType();

    static {
        FIELD_TYPE.setStored(false);
        FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
        FIELD_TYPE.freeze();
    }

    public static class Builder extends AbstractPointGeometryFieldMapper.Builder<Builder, GeoPointFieldType> {

        public Builder(String name) {
            super(name, FIELD_TYPE);
            hasDocValues = true;
            builder = this;
        }

        @Override
        public GeoPointFieldMapper build(BuilderContext context, String simpleName, FieldType fieldType,
                                         MultiFields multiFields, Explicit<Boolean> ignoreMalformed,
                                         Explicit<Boolean> ignoreZValue, ParsedPoint nullValue, CopyTo copyTo) {
            GeoPointFieldType ft = new GeoPointFieldType(buildFullName(context), indexed, hasDocValues, meta);
            ft.setGeometryParser(new PointParser<>());
            ft.setGeometryIndexer(new GeoPointIndexer(ft));
            ft.setGeometryQueryBuilder(new VectorGeoPointShapeQueryProcessor());
            return new GeoPointFieldMapper(name, fieldType, ft, multiFields, ignoreMalformed, ignoreZValue, nullValue, copyTo);
        }
    }

    public static class TypeParser extends AbstractPointGeometryFieldMapper.TypeParser<Builder> {
        @Override
        protected Builder newBuilder(String name, Map<String, Object> params) {
            return new GeoPointFieldMapper.Builder(name);
        }

        protected ParsedGeoPoint parseNullValue(Object nullValue, boolean ignoreZValue, boolean ignoreMalformed) {
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
    }

    /**
     * Parses geopoint represented as an object or an array, ignores malformed geopoints if needed
     */
    @Override
    protected void parsePointIgnoringMalformed(XContentParser parser, ParsedPoint point) throws IOException {
        GeoUtils.parseGeoPoint(parser, (GeoPoint)point, ignoreZValue().value());
        super.parsePointIgnoringMalformed(parser, point);
    }

    public GeoPointFieldMapper(String simpleName, FieldType fieldType, MappedFieldType mappedFieldType,
                               MultiFields multiFields, Explicit<Boolean> ignoreMalformed,
                               Explicit<Boolean> ignoreZValue, ParsedPoint nullValue, CopyTo copyTo) {
        super(simpleName, fieldType, mappedFieldType, multiFields, ignoreMalformed, ignoreZValue, nullValue, copyTo);
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

    @Override
    public GeoPointFieldType fieldType() {
        return (GeoPointFieldType)mappedFieldType;
    }

    @Override
    protected ParsedPoint newParsedPoint() {
        return new ParsedGeoPoint();
    }

    public static class GeoPointFieldType extends AbstractPointGeometryFieldType<List<ParsedGeoPoint>, List<? extends GeoPoint>> {
        public GeoPointFieldType(String name, boolean indexed, boolean hasDocValues, Map<String, String> meta) {
            super(name, indexed, hasDocValues, meta);
        }

        public GeoPointFieldType(String name) {
            this(name, true, true, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
            failIfNoDocValues();
            return new AbstractLatLonPointIndexFieldData.Builder(name(), CoreValuesSourceType.GEOPOINT);
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
            } else if (other instanceof ParsedGeoPoint == false) {
                return false;
            } else {
                ParsedGeoPoint o = (ParsedGeoPoint)other;
                oLat = o.lat();
                oLon = o.lon();
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
