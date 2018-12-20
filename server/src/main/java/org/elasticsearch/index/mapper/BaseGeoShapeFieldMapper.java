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

import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.Version;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilder.Orientation;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.mapper.LegacyGeoShapeFieldMapper.DeprecatedParameters;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.index.mapper.GeoPointFieldMapper.Names.IGNORE_MALFORMED;

/**
 * Base class for {@link GeoShapeFieldMapper} and {@link LegacyGeoShapeFieldMapper}
 */
public abstract class BaseGeoShapeFieldMapper extends FieldMapper {
    public static final String CONTENT_TYPE = "geo_shape";

    public static class Names {
        public static final ParseField ORIENTATION = new ParseField("orientation");
        public static final ParseField COERCE = new ParseField("coerce");
    }

    public static class Defaults {
        public static final Explicit<Orientation> ORIENTATION = new Explicit<>(Orientation.RIGHT, false);
        public static final Explicit<Boolean> COERCE = new Explicit<>(false, false);
        public static final Explicit<Boolean> IGNORE_MALFORMED = new Explicit<>(false, false);
        public static final Explicit<Boolean> IGNORE_Z_VALUE = new Explicit<>(true, false);
    }

    public abstract static class Builder<T extends Builder, Y extends BaseGeoShapeFieldMapper>
            extends FieldMapper.Builder<T, Y> {
        protected Boolean coerce;
        protected Boolean ignoreMalformed;
        protected Boolean ignoreZValue;
        protected Orientation orientation;

        /** default builder - used for external mapper*/
        public Builder(String name, MappedFieldType fieldType, MappedFieldType defaultFieldType) {
            super(name, fieldType, defaultFieldType);
        }

        public Builder(String name, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                       boolean coerce, boolean ignoreMalformed, Orientation orientation, boolean ignoreZ) {
            super(name, fieldType, defaultFieldType);
            this.coerce = coerce;
            this.ignoreMalformed = ignoreMalformed;
            this.orientation = orientation;
            this.ignoreZValue = ignoreZ;
        }

        public Builder coerce(boolean coerce) {
            this.coerce = coerce;
            return this;
        }

        protected Explicit<Boolean> coerce(BuilderContext context) {
            if (coerce != null) {
                return new Explicit<>(coerce, true);
            }
            if (context.indexSettings() != null) {
                return new Explicit<>(COERCE_SETTING.get(context.indexSettings()), false);
            }
            return Defaults.COERCE;
        }

        public Builder orientation(Orientation orientation) {
            this.orientation = orientation;
            return this;
        }

        protected Explicit<Orientation> orientation() {
            if (orientation != null) {
                return new Explicit<>(orientation, true);
            }
            return Defaults.ORIENTATION;
        }

        @Override
        protected boolean defaultDocValues(Version indexCreated) {
            return false;
        }

        public Builder ignoreMalformed(boolean ignoreMalformed) {
            this.ignoreMalformed = ignoreMalformed;
            return this;
        }

        protected Explicit<Boolean> ignoreMalformed(BuilderContext context) {
            if (ignoreMalformed != null) {
                return new Explicit<>(ignoreMalformed, true);
            }
            if (context.indexSettings() != null) {
                return new Explicit<>(IGNORE_MALFORMED_SETTING.get(context.indexSettings()), false);
            }
            return Defaults.IGNORE_MALFORMED;
        }

        protected Explicit<Boolean> ignoreZValue() {
            if (ignoreZValue != null) {
                return new Explicit<>(ignoreZValue, true);
            }
            return Defaults.IGNORE_Z_VALUE;
        }

        public Builder ignoreZValue(final boolean ignoreZValue) {
            this.ignoreZValue = ignoreZValue;
            return this;
        }

        @Override
        protected void setupFieldType(BuilderContext context) {
            super.setupFieldType(context);

            // field mapper handles this at build time
            // but prefix tree strategies require a name, so throw a similar exception
            if (name().isEmpty()) {
                throw new IllegalArgumentException("name cannot be empty string");
            }

            BaseGeoShapeFieldType ft = (BaseGeoShapeFieldType)fieldType();
            ft.setOrientation(orientation().value());
        }
    }

    public static class TypeParser implements Mapper.TypeParser {

        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Boolean coerce = null;
            Boolean ignoreZ = null;
            Boolean ignoreMalformed = null;
            Orientation orientation = null;
            DeprecatedParameters deprecatedParameters = new DeprecatedParameters();
            boolean parsedDeprecatedParams = false;
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = entry.getKey();
                Object fieldNode = entry.getValue();
                if (DeprecatedParameters.parse(name, fieldName, fieldNode, deprecatedParameters)) {
                    parsedDeprecatedParams = true;
                    iterator.remove();
                } else if (Names.ORIENTATION.match(fieldName, LoggingDeprecationHandler.INSTANCE)) {
                    orientation = ShapeBuilder.Orientation.fromString(fieldNode.toString());
                    iterator.remove();
                } else if (IGNORE_MALFORMED.equals(fieldName)) {
                    ignoreMalformed = XContentMapValues.nodeBooleanValue(fieldNode, name + ".ignore_malformed");
                    iterator.remove();
                } else if (Names.COERCE.match(fieldName, LoggingDeprecationHandler.INSTANCE)) {
                    coerce = XContentMapValues.nodeBooleanValue(fieldNode, name + "." + Names.COERCE.getPreferredName());
                    iterator.remove();
                } else if (GeoPointFieldMapper.Names.IGNORE_Z_VALUE.getPreferredName().equals(fieldName)) {
                    ignoreZ = XContentMapValues.nodeBooleanValue(fieldNode,
                        name + "." + GeoPointFieldMapper.Names.IGNORE_Z_VALUE.getPreferredName());
                    iterator.remove();
                }
            }
            final Builder builder;
            if (parsedDeprecatedParams || parserContext.indexVersionCreated().before(Version.V_7_0_0)) {
                // Legacy index-based shape
                builder = new LegacyGeoShapeFieldMapper.Builder(name, deprecatedParameters);
            } else {
                // BKD-based shape
                builder = new GeoShapeFieldMapper.Builder(name);
            }

            if (coerce != null) {
                builder.coerce(coerce);
            }

            if (ignoreZ != null) {
                builder.ignoreZValue(ignoreZ);
            }

            if (ignoreMalformed != null) {
                builder.ignoreMalformed(ignoreMalformed);
            }

            if (orientation != null) {
                builder.orientation(orientation);
            }

            return builder;
        }
    }

    public abstract static class BaseGeoShapeFieldType extends MappedFieldType {
        protected Orientation orientation = Defaults.ORIENTATION.value();

        protected BaseGeoShapeFieldType() {
            setIndexOptions(IndexOptions.DOCS);
            setTokenized(false);
            setStored(false);
            setStoreTermVectors(false);
            setOmitNorms(true);
        }

        protected BaseGeoShapeFieldType(BaseGeoShapeFieldType ref) {
            super(ref);
            this.orientation = ref.orientation;
        }

        @Override
        public boolean equals(Object o) {
            if (!super.equals(o)) return false;
            BaseGeoShapeFieldType that = (BaseGeoShapeFieldType) o;
            return orientation == that.orientation;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), orientation);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public void checkCompatibility(MappedFieldType fieldType, List<String> conflicts) {
            super.checkCompatibility(fieldType, conflicts);
        }

        public Orientation orientation() { return this.orientation; }

        public void setOrientation(Orientation orientation) {
            checkIfFrozen();
            this.orientation = orientation;
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            return new TermQuery(new Term(FieldNamesFieldMapper.NAME, name()));
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new QueryShardException(context, "Geo fields do not support exact searching, use dedicated geo queries instead");
        }
    }

    protected Explicit<Boolean> coerce;
    protected Explicit<Boolean> ignoreMalformed;
    protected Explicit<Boolean> ignoreZValue;

    protected BaseGeoShapeFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                                     Explicit<Boolean> ignoreMalformed, Explicit<Boolean> coerce,
                                     Explicit<Boolean> ignoreZValue, Settings indexSettings,
                                     MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
        this.coerce = coerce;
        this.ignoreMalformed = ignoreMalformed;
        this.ignoreZValue = ignoreZValue;
    }

    @Override
    protected void doMerge(Mapper mergeWith) {
        super.doMerge(mergeWith);
        BaseGeoShapeFieldMapper gsfm = (BaseGeoShapeFieldMapper)mergeWith;
        if (gsfm.coerce.explicit()) {
            this.coerce = gsfm.coerce;
        }
        if (gsfm.ignoreMalformed.explicit()) {
            this.ignoreMalformed = gsfm.ignoreMalformed;
        }
        if (gsfm.ignoreZValue.explicit()) {
            this.ignoreZValue = gsfm.ignoreZValue;
        }
    }

    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        builder.field("type", contentType());
        BaseGeoShapeFieldType ft = (BaseGeoShapeFieldType)fieldType();
        if (includeDefaults || ft.orientation() != Defaults.ORIENTATION.value()) {
            builder.field(Names.ORIENTATION.getPreferredName(), ft.orientation());
        }
        if (includeDefaults || coerce.explicit()) {
            builder.field(Names.COERCE.getPreferredName(), coerce.value());
        }
        if (includeDefaults || ignoreMalformed.explicit()) {
            builder.field(IGNORE_MALFORMED, ignoreMalformed.value());
        }
        if (includeDefaults || ignoreZValue.explicit()) {
            builder.field(GeoPointFieldMapper.Names.IGNORE_Z_VALUE.getPreferredName(), ignoreZValue.value());
        }
    }

    public Explicit<Boolean> coerce() {
        return coerce;
    }

    public Explicit<Boolean> ignoreMalformed() {
        return ignoreMalformed;
    }

    public Explicit<Boolean> ignoreZValue() {
        return ignoreZValue;
    }

    public Orientation orientation() {
        return ((BaseGeoShapeFieldType)fieldType).orientation();
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}
