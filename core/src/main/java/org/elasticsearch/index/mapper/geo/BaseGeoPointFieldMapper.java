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

package org.elasticsearch.index.mapper.geo;

import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.util.GeoHashUtils;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.Version;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.core.DoubleFieldMapper;
import org.elasticsearch.index.mapper.core.NumberFieldMapper;
import org.elasticsearch.index.mapper.core.StringFieldMapper;
import org.elasticsearch.index.mapper.object.ArrayValueMapperParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.MapperBuilders.doubleField;
import static org.elasticsearch.index.mapper.MapperBuilders.stringField;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseField;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseMultiField;

/**
 * GeoPointFieldMapper base class to maintain backward compatibility
 */
public abstract class BaseGeoPointFieldMapper extends FieldMapper implements ArrayValueMapperParser {
    public static final String CONTENT_TYPE = "geo_point";

    public static class Names {
        public static final String LAT = "lat";
        public static final String LAT_SUFFIX = "." + LAT;
        public static final String LON = "lon";
        public static final String LON_SUFFIX = "." + LON;
        public static final String GEOHASH = "geohash";
        public static final String GEOHASH_SUFFIX = "." + GEOHASH;
        public static final String IGNORE_MALFORMED = "ignore_malformed";
    }

    public static class Defaults {
        public static final boolean ENABLE_LATLON = false;
        public static final boolean ENABLE_GEOHASH = false;
        public static final boolean ENABLE_GEOHASH_PREFIX = false;
        public static final int GEO_HASH_PRECISION = GeoHashUtils.PRECISION;
        public static final Explicit<Boolean> IGNORE_MALFORMED = new Explicit(false, false);
    }

    public abstract static class Builder<T extends Builder, Y extends BaseGeoPointFieldMapper> extends FieldMapper.Builder<T, Y> {

        protected boolean enableLatLon = Defaults.ENABLE_LATLON;

        protected Integer precisionStep;

        protected boolean enableGeoHash = Defaults.ENABLE_GEOHASH;

        protected boolean enableGeoHashPrefix = Defaults.ENABLE_GEOHASH_PREFIX;

        protected int geoHashPrecision = Defaults.GEO_HASH_PRECISION;

        protected Boolean ignoreMalformed;

        public Builder(String name, GeoPointFieldType fieldType) {
            super(name, fieldType, fieldType);
        }

        @Override
        public GeoPointFieldType fieldType() {
            return (GeoPointFieldType)fieldType;
        }

        @Override
        public T fieldDataSettings(Settings settings) {
            this.fieldDataSettings = settings;
            return builder;
        }

        public T enableLatLon(boolean enableLatLon) {
            this.enableLatLon = enableLatLon;
            return builder;
        }

        public T precisionStep(int precisionStep) {
            this.precisionStep = precisionStep;
            return builder;
        }

        public T enableGeoHash(boolean enableGeoHash) {
            this.enableGeoHash = enableGeoHash;
            return builder;
        }

        public T geoHashPrefix(boolean enableGeoHashPrefix) {
            this.enableGeoHashPrefix = enableGeoHashPrefix;
            return builder;
        }

        public T geoHashPrecision(int precision) {
            this.geoHashPrecision = precision;
            return builder;
        }

        public T ignoreMalformed(boolean ignoreMalformed) {
            this.ignoreMalformed = ignoreMalformed;
            return builder;
        }

        protected Explicit<Boolean> ignoreMalformed(BuilderContext context) {
            if (ignoreMalformed != null) {
                return new Explicit<>(ignoreMalformed, true);
            }
            if (context.indexSettings() != null) {
                return new Explicit<>(context.indexSettings().getAsBoolean("index.mapping.ignore_malformed", Defaults.IGNORE_MALFORMED.value()), false);
            }
            return Defaults.IGNORE_MALFORMED;
        }

        public abstract Y build(BuilderContext context, String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                                Settings indexSettings, DoubleFieldMapper latMapper, DoubleFieldMapper lonMapper,
                                StringFieldMapper geoHashMapper, MultiFields multiFields, Explicit<Boolean> ignoreMalformed, CopyTo copyTo);

        public Y build(Mapper.BuilderContext context) {
            GeoPointFieldType geoPointFieldType = (GeoPointFieldType)fieldType;

            DoubleFieldMapper latMapper = null;
            DoubleFieldMapper lonMapper = null;

            context.path().add(name);
            if (enableLatLon) {
                NumberFieldMapper.Builder<?, ?> latMapperBuilder = doubleField(Names.LAT).includeInAll(false);
                NumberFieldMapper.Builder<?, ?> lonMapperBuilder = doubleField(Names.LON).includeInAll(false);
                if (precisionStep != null) {
                    latMapperBuilder.precisionStep(precisionStep);
                    lonMapperBuilder.precisionStep(precisionStep);
                }
                latMapper = (DoubleFieldMapper) latMapperBuilder.includeInAll(false).store(fieldType.stored()).docValues(false).build(context);
                lonMapper = (DoubleFieldMapper) lonMapperBuilder.includeInAll(false).store(fieldType.stored()).docValues(false).build(context);
                geoPointFieldType.setLatLonEnabled(latMapper.fieldType(), lonMapper.fieldType());
            }
            StringFieldMapper geoHashMapper = null;
            if (enableGeoHash || enableGeoHashPrefix) {
                // TODO: possible also implicitly enable geohash if geohash precision is set
                geoHashMapper = stringField(Names.GEOHASH).index(true).tokenized(false).includeInAll(false).store(fieldType.stored())
                        .omitNorms(true).indexOptions(IndexOptions.DOCS).build(context);
                geoPointFieldType.setGeoHashEnabled(geoHashMapper.fieldType(), geoHashPrecision, enableGeoHashPrefix);
            }
            context.path().remove();

            return build(context, name, fieldType, defaultFieldType, context.indexSettings(),
                    latMapper, lonMapper, geoHashMapper, multiFieldsBuilder.build(this, context), ignoreMalformed(context), copyTo);
        }
    }

    public abstract static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<?, ?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder builder;
            if (parserContext.indexVersionCreated().before(Version.V_2_2_0)) {
                builder = new GeoPointFieldMapperLegacy.Builder(name);
            } else {
                builder = new GeoPointFieldMapper.Builder(name);
            }
            parseField(builder, name, node, parserContext);

            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = Strings.toUnderscoreCase(entry.getKey());
                Object propNode = entry.getValue();
                if (propName.equals("lat_lon")) {
                    builder.enableLatLon(XContentMapValues.nodeBooleanValue(propNode));
                    iterator.remove();
                } else if (propName.equals("precision_step")) {
                    builder.precisionStep(XContentMapValues.nodeIntegerValue(propNode));
                    iterator.remove();
                } else if (propName.equals("geohash")) {
                    builder.enableGeoHash(XContentMapValues.nodeBooleanValue(propNode));
                    iterator.remove();
                } else if (propName.equals("geohash_prefix")) {
                    builder.geoHashPrefix(XContentMapValues.nodeBooleanValue(propNode));
                    if (XContentMapValues.nodeBooleanValue(propNode)) {
                        builder.enableGeoHash(true);
                    }
                    iterator.remove();
                } else if (propName.equals("geohash_precision")) {
                    if (propNode instanceof Integer) {
                        builder.geoHashPrecision(XContentMapValues.nodeIntegerValue(propNode));
                    } else {
                        builder.geoHashPrecision(GeoUtils.geoHashLevelsForPrecision(propNode.toString()));
                    }
                    iterator.remove();
                } else if (propName.equals(Names.IGNORE_MALFORMED)) {
                    builder.ignoreMalformed(XContentMapValues.nodeBooleanValue(propNode));
                    iterator.remove();
                } else if (parseMultiField(builder, name, parserContext, propName, propNode)) {
                    iterator.remove();
                }
            }

            if (builder instanceof GeoPointFieldMapperLegacy.Builder) {
                return GeoPointFieldMapperLegacy.parse((GeoPointFieldMapperLegacy.Builder) builder, node, parserContext);
            }

            return (GeoPointFieldMapper.Builder) builder;
        }
    }

    public static class GeoPointFieldType extends MappedFieldType {
        protected MappedFieldType geoHashFieldType;
        protected int geoHashPrecision;
        protected boolean geoHashPrefixEnabled;

        protected MappedFieldType latFieldType;
        protected MappedFieldType lonFieldType;

        GeoPointFieldType() {}

        GeoPointFieldType(GeoPointFieldType ref) {
            super(ref);
            this.geoHashFieldType = ref.geoHashFieldType; // copying ref is ok, this can never be modified
            this.geoHashPrecision = ref.geoHashPrecision;
            this.geoHashPrefixEnabled = ref.geoHashPrefixEnabled;
            this.latFieldType = ref.latFieldType; // copying ref is ok, this can never be modified
            this.lonFieldType = ref.lonFieldType; // copying ref is ok, this can never be modified
        }

        @Override
        public MappedFieldType clone() {
            return new GeoPointFieldType(this);
        }

        @Override
        public boolean equals(Object o) {
            if (!super.equals(o)) return false;
            GeoPointFieldType that = (GeoPointFieldType) o;
            return  geoHashPrecision == that.geoHashPrecision &&
                    geoHashPrefixEnabled == that.geoHashPrefixEnabled &&
                    java.util.Objects.equals(geoHashFieldType, that.geoHashFieldType) &&
                    java.util.Objects.equals(latFieldType, that.latFieldType) &&
                    java.util.Objects.equals(lonFieldType, that.lonFieldType);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(super.hashCode(), geoHashFieldType, geoHashPrecision, geoHashPrefixEnabled, latFieldType,
                    lonFieldType);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public void checkCompatibility(MappedFieldType fieldType, List<String> conflicts, boolean strict) {
            super.checkCompatibility(fieldType, conflicts, strict);
            GeoPointFieldType other = (GeoPointFieldType)fieldType;
            if (isLatLonEnabled() != other.isLatLonEnabled()) {
                conflicts.add("mapper [" + name() + "] has different [lat_lon]");
            }
            if (isLatLonEnabled() && other.isLatLonEnabled() &&
                    latFieldType().numericPrecisionStep() != other.latFieldType().numericPrecisionStep()) {
                conflicts.add("mapper [" + name() + "] has different [precision_step]");
            }
            if (isGeoHashEnabled() != other.isGeoHashEnabled()) {
                conflicts.add("mapper [" + name() + "] has different [geohash]");
            }
            if (geoHashPrecision() != other.geoHashPrecision()) {
                conflicts.add("mapper [" + name() + "] has different [geohash_precision]");
            }
            if (isGeoHashPrefixEnabled() != other.isGeoHashPrefixEnabled()) {
                conflicts.add("mapper [" + name() + "] has different [geohash_prefix]");
            }
        }

        public boolean isGeoHashEnabled() {
            return geoHashFieldType != null;
        }

        public MappedFieldType geoHashFieldType() {
            return geoHashFieldType;
        }

        public int geoHashPrecision() {
            return geoHashPrecision;
        }

        public boolean isGeoHashPrefixEnabled() {
            return geoHashPrefixEnabled;
        }

        public void setGeoHashEnabled(MappedFieldType geoHashFieldType, int geoHashPrecision, boolean geoHashPrefixEnabled) {
            checkIfFrozen();
            this.geoHashFieldType = geoHashFieldType;
            this.geoHashPrecision = geoHashPrecision;
            this.geoHashPrefixEnabled = geoHashPrefixEnabled;
        }

        public boolean isLatLonEnabled() {
            return latFieldType != null;
        }

        public MappedFieldType latFieldType() {
            return latFieldType;
        }

        public MappedFieldType lonFieldType() {
            return lonFieldType;
        }

        public void setLatLonEnabled(MappedFieldType latFieldType, MappedFieldType lonFieldType) {
            checkIfFrozen();
            this.latFieldType = latFieldType;
            this.lonFieldType = lonFieldType;
        }
    }

    protected DoubleFieldMapper latMapper;

    protected DoubleFieldMapper lonMapper;

    protected StringFieldMapper geoHashMapper;

    protected Explicit<Boolean> ignoreMalformed;

    protected BaseGeoPointFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType, Settings indexSettings,
                                      DoubleFieldMapper latMapper, DoubleFieldMapper lonMapper, StringFieldMapper geoHashMapper,
                                      MultiFields multiFields, Explicit<Boolean> ignoreMalformed, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
        this.latMapper = latMapper;
        this.lonMapper = lonMapper;
        this.geoHashMapper = geoHashMapper;
        this.ignoreMalformed = ignoreMalformed;
    }

    @Override
    public GeoPointFieldType fieldType() {
        return (GeoPointFieldType) super.fieldType();
    }

    @Override
    protected void doMerge(Mapper mergeWith, boolean updateAllTypes) {
        super.doMerge(mergeWith, updateAllTypes);
        BaseGeoPointFieldMapper gpfmMergeWith = (BaseGeoPointFieldMapper) mergeWith;
        if (gpfmMergeWith.ignoreMalformed.explicit()) {
            this.ignoreMalformed = gpfmMergeWith.ignoreMalformed;
        }
    }

    @Override
    public Iterator<Mapper> iterator() {
        List<Mapper> extras = new ArrayList<>();
        if (fieldType().isGeoHashEnabled()) {
            extras.add(geoHashMapper);
        }
        if (fieldType().isLatLonEnabled()) {
            extras.add(latMapper);
            extras.add(lonMapper);
        }
        return Iterators.concat(super.iterator(), extras.iterator());
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        throw new UnsupportedOperationException("Parsing is implemented in parse(), this method should NEVER be called");
    }

    protected void parse(ParseContext context, GeoPoint point, String geoHash) throws IOException {
        if (fieldType().isGeoHashEnabled()) {
            if (geoHash == null) {
                geoHash = GeoHashUtils.stringEncode(point.lon(), point.lat());
            }
            addGeoHashField(context, geoHash);
        }
        if (fieldType().isLatLonEnabled()) {
            latMapper.parse(context.createExternalValueContext(point.lat()));
            lonMapper.parse(context.createExternalValueContext(point.lon()));
        }
        multiFields.parse(this, context);
    }

    @Override
    public Mapper parse(ParseContext context) throws IOException {
        context.path().add(simpleName());

        GeoPoint sparse = context.parseExternalValue(GeoPoint.class);

        if (sparse != null) {
            parse(context, sparse, null);
        } else {
            sparse = new GeoPoint();
            XContentParser.Token token = context.parser().currentToken();
            if (token == XContentParser.Token.START_ARRAY) {
                token = context.parser().nextToken();
                if (token == XContentParser.Token.START_ARRAY) {
                    // its an array of array of lon/lat [ [1.2, 1.3], [1.4, 1.5] ]
                    while (token != XContentParser.Token.END_ARRAY) {
                        parse(context, GeoUtils.parseGeoPoint(context.parser(), sparse), null);
                        token = context.parser().nextToken();
                    }
                } else {
                    // its an array of other possible values
                    if (token == XContentParser.Token.VALUE_NUMBER) {
                        double lon = context.parser().doubleValue();
                        token = context.parser().nextToken();
                        double lat = context.parser().doubleValue();
                        while ((token = context.parser().nextToken()) != XContentParser.Token.END_ARRAY);
                        parse(context, sparse.reset(lat, lon), null);
                    } else {
                        while (token != XContentParser.Token.END_ARRAY) {
                            if (token == XContentParser.Token.VALUE_STRING) {
                                parsePointFromString(context, sparse, context.parser().text());
                            } else {
                                parse(context, GeoUtils.parseGeoPoint(context.parser(), sparse), null);
                            }
                            token = context.parser().nextToken();
                        }
                    }
                }
            } else if (token == XContentParser.Token.VALUE_STRING) {
                parsePointFromString(context, sparse, context.parser().text());
            } else if (token != XContentParser.Token.VALUE_NULL) {
                parse(context, GeoUtils.parseGeoPoint(context.parser(), sparse), null);
            }
        }

        context.path().remove();
        return null;
    }

    private void addGeoHashField(ParseContext context, String geoHash) throws IOException {
        int len = Math.min(fieldType().geoHashPrecision(), geoHash.length());
        int min = fieldType().isGeoHashPrefixEnabled() ? 1 : len;

        for (int i = len; i >= min; i--) {
            // side effect of this call is adding the field
            geoHashMapper.parse(context.createExternalValueContext(geoHash.substring(0, i)));
        }
    }

    private void parsePointFromString(ParseContext context, GeoPoint sparse, String point) throws IOException {
        if (point.indexOf(',') < 0) {
            parse(context, sparse.resetFromGeoHash(point), point);
        } else {
            parse(context, sparse.resetFromString(point), null);
        }
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
        if (includeDefaults || fieldType().isLatLonEnabled() != GeoPointFieldMapper.Defaults.ENABLE_LATLON) {
            builder.field("lat_lon", fieldType().isLatLonEnabled());
        }
        if (fieldType().isLatLonEnabled() && (includeDefaults || fieldType().latFieldType().numericPrecisionStep() != NumericUtils.PRECISION_STEP_DEFAULT)) {
            builder.field("precision_step", fieldType().latFieldType().numericPrecisionStep());
        }
        if (includeDefaults || fieldType().isGeoHashEnabled() != Defaults.ENABLE_GEOHASH) {
            builder.field("geohash", fieldType().isGeoHashEnabled());
        }
        if (includeDefaults || fieldType().isGeoHashPrefixEnabled() != Defaults.ENABLE_GEOHASH_PREFIX) {
            builder.field("geohash_prefix", fieldType().isGeoHashPrefixEnabled());
        }
        if (fieldType().isGeoHashEnabled() && (includeDefaults || fieldType().geoHashPrecision() != Defaults.GEO_HASH_PRECISION)) {
            builder.field("geohash_precision", fieldType().geoHashPrecision());
        }
        if (includeDefaults || ignoreMalformed.explicit()) {
            builder.field(Names.IGNORE_MALFORMED, ignoreMalformed.value());
        }
    }

    @Override
    public FieldMapper updateFieldType(Map<String, MappedFieldType> fullNameToFieldType) {
        BaseGeoPointFieldMapper updated = (BaseGeoPointFieldMapper) super.updateFieldType(fullNameToFieldType);
        StringFieldMapper geoUpdated = geoHashMapper == null ? null : (StringFieldMapper) geoHashMapper.updateFieldType(fullNameToFieldType);
        DoubleFieldMapper latUpdated = latMapper == null ? null : (DoubleFieldMapper) latMapper.updateFieldType(fullNameToFieldType);
        DoubleFieldMapper lonUpdated = lonMapper == null ? null : (DoubleFieldMapper) lonMapper.updateFieldType(fullNameToFieldType);
        if (updated == this
                && geoUpdated == geoHashMapper
                && latUpdated == latMapper
                && lonUpdated == lonMapper) {
            return this;
        }
        if (updated == this) {
            updated = (BaseGeoPointFieldMapper) updated.clone();
        }
        updated.geoHashMapper = geoUpdated;
        updated.latMapper = latUpdated;
        updated.lonMapper = lonUpdated;
        return updated;
    }
}
