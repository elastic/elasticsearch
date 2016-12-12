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
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.AbstractLatLonPointDVIndexFieldData;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;

import java.io.IOException;
import java.util.Map;

/**
 * Field Mapper for geo_point types.
 *
 * Uses lucene 6 LatLonPoint encoding
 */
public class LatLonPointFieldMapper extends BaseGeoPointFieldMapper {
    public static final String CONTENT_TYPE = "geo_point";

    public static class Defaults extends BaseGeoPointFieldMapper.Defaults {
        public static final LatLonPointFieldType FIELD_TYPE = new LatLonPointFieldType();

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setHasDocValues(true);
            FIELD_TYPE.setDimensions(2, Integer.BYTES);
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends BaseGeoPointFieldMapper.Builder<Builder, LatLonPointFieldMapper> {
        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE);
            builder = this;
        }

        @Override
        public LatLonPointFieldMapper build(BuilderContext context, String simpleName, MappedFieldType fieldType,
                                            MappedFieldType defaultFieldType, Settings indexSettings,
                                            FieldMapper latMapper, FieldMapper lonMapper, FieldMapper geoHashMapper,
                                            MultiFields multiFields, Explicit<Boolean> ignoreMalformed,
                                            CopyTo copyTo) {
            setupFieldType(context);
            return new LatLonPointFieldMapper(simpleName, fieldType, defaultFieldType, indexSettings, multiFields,
                ignoreMalformed, copyTo);
        }

        @Override
        public LatLonPointFieldMapper build(BuilderContext context) {
            return super.build(context);
        }
    }

    public static class TypeParser extends BaseGeoPointFieldMapper.TypeParser {
        @Override
        public Mapper.Builder<?, ?> parse(String name, Map<String, Object> node, ParserContext parserContext)
                throws MapperParsingException {
            return super.parse(name, node, parserContext);
        }
    }

    public LatLonPointFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                                  Settings indexSettings, MultiFields multiFields, Explicit<Boolean> ignoreMalformed,
                                  CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, null, null, null, multiFields, ignoreMalformed, copyTo);
    }

    public static class LatLonPointFieldType extends GeoPointFieldType {
        LatLonPointFieldType() {
        }

        LatLonPointFieldType(LatLonPointFieldType ref) {
            super(ref);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public MappedFieldType clone() {
            return new LatLonPointFieldType(this);
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder() {
            failIfNoDocValues();
            return new AbstractLatLonPointDVIndexFieldData.Builder();
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new QueryShardException(context, "Geo fields do not support exact searching, use dedicated geo queries instead: ["
                + name() + "]");
        }
    }

    @Override
    protected void parse(ParseContext originalContext, GeoPoint point, String geoHash) throws IOException {
        // Geopoint fields, by default, will not be included in _all
        final ParseContext context = originalContext.setIncludeInAllDefault(false);

        if (ignoreMalformed.value() == false) {
            if (point.lat() > 90.0 || point.lat() < -90.0) {
                throw new IllegalArgumentException("illegal latitude value [" + point.lat() + "] for " + name());
            }
            if (point.lon() > 180.0 || point.lon() < -180) {
                throw new IllegalArgumentException("illegal longitude value [" + point.lon() + "] for " + name());
            }
        } else {
            GeoUtils.normalizePoint(point);
        }
        if (fieldType().indexOptions() != IndexOptions.NONE) {
            context.doc().add(new LatLonPoint(fieldType().name(), point.lat(), point.lon()));
        }
        if (fieldType().stored()) {
            context.doc().add(new StoredField(fieldType().name(), point.toString()));
        }
        if (fieldType.hasDocValues()) {
            context.doc().add(new LatLonDocValuesField(fieldType().name(), point.lat(), point.lon()));
        }
        // if the mapping contains multifields then use the geohash string
        if (multiFields.iterator().hasNext()) {
            multiFields.parse(this, context.createExternalValueContext(point.geohash()));
        }
    }
}
