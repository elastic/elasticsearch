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
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilder.Orientation;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.mapper.LegacyGeoShapeFieldMapper.DeprecatedParameters;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Base class for {@link GeoShapeFieldMapper} and {@link LegacyGeoShapeFieldMapper}
 */
public abstract class AbstractShapeGeometryFieldMapper<Parsed, Processed> extends AbstractGeometryFieldMapper<Parsed, Processed> {

    public static class Names extends AbstractGeometryFieldMapper.Names {
        public static final ParseField ORIENTATION = new ParseField("orientation");
        public static final ParseField COERCE = new ParseField("coerce");
    }

    public static class Defaults extends AbstractGeometryFieldMapper.Defaults {
        public static final Explicit<Orientation> ORIENTATION = new Explicit<>(Orientation.RIGHT, false);
        public static final Explicit<Boolean> COERCE = new Explicit<>(false, false);
    }

    public abstract static class Builder<T extends Builder<T, FT>,
            FT extends AbstractShapeGeometryFieldType> extends AbstractGeometryFieldMapper.Builder<T, FT> {
        protected Boolean coerce;
        protected Orientation orientation;

        /** default builder - used for external mapper*/
        public Builder(String name, FieldType fieldType) {
            super(name, fieldType);
        }

        public Builder(String name, FieldType fieldType,
                       boolean coerce, boolean ignoreMalformed, Orientation orientation, boolean ignoreZ) {
            super(name, fieldType, ignoreMalformed, ignoreZ);
            this.coerce = coerce;
            this.orientation = orientation;
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

        protected Explicit<Boolean> coerce() {
            if (coerce != null) {
                return new Explicit<>(coerce, true);
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

    }

    protected static final String DEPRECATED_PARAMETERS_KEY = "deprecated_parameters";

    public abstract static class TypeParser extends AbstractGeometryFieldMapper.TypeParser<Builder> {
        protected abstract Builder newBuilder(String name, Map<String, Object> params);

        protected boolean parseXContentParameters(String name, Map.Entry<String, Object> entry, Map<String, Object> params)
                throws MapperParsingException {
            if (DeprecatedParameters.parse(name, entry.getKey(), entry.getValue(),
                (DeprecatedParameters)params.get(DEPRECATED_PARAMETERS_KEY))) {
                return true;
            }
            return false;
        }

        @Override
        public Builder parse(String name, Map<String, Object> node, Map<String, Object> params, ParserContext parserContext) {
            boolean parsedDeprecatedParameters = false;
            params.put(DEPRECATED_PARAMETERS_KEY, new DeprecatedParameters());
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = entry.getKey();
                Object fieldNode = entry.getValue();
                if (parseXContentParameters(name, entry, params)) {
                    parsedDeprecatedParameters = true;
                    iterator.remove();
                } else if (Names.ORIENTATION.match(fieldName, LoggingDeprecationHandler.INSTANCE)) {
                    params.put(Names.ORIENTATION.getPreferredName(), ShapeBuilder.Orientation.fromString(fieldNode.toString()));
                    iterator.remove();
                } else if (Names.COERCE.match(fieldName, LoggingDeprecationHandler.INSTANCE)) {
                    params.put(Names.COERCE.getPreferredName(),
                        XContentMapValues.nodeBooleanValue(fieldNode, name + "." + Names.COERCE.getPreferredName()));
                    iterator.remove();
                }
            }
            if (parsedDeprecatedParameters == false) {
                params.remove(DEPRECATED_PARAMETERS_KEY);
            }

            Builder builder = super.parse(name, node, params, parserContext);

            if (params.containsKey(Names.COERCE.getPreferredName())) {
                builder.coerce((Boolean)params.get(Names.COERCE.getPreferredName()));
            }

            if (params.containsKey(Names.ORIENTATION.getPreferredName())) {
                builder.orientation((Orientation)params.get(Names.ORIENTATION.getPreferredName()));
            }

            return builder;
        }
    }

    public abstract static class AbstractShapeGeometryFieldType<Parsed, Processed> extends AbstractGeometryFieldType<Parsed, Processed> {
        protected Orientation orientation = Defaults.ORIENTATION.value();

        protected AbstractShapeGeometryFieldType(String name, boolean isSearchable, boolean hasDocValues, Map<String, String> meta) {
            super(name, isSearchable, hasDocValues, meta);
        }

        public Orientation orientation() { return this.orientation; }

        public void setOrientation(Orientation orientation) {
            this.orientation = orientation;
        }
    }

    protected Explicit<Boolean> coerce;
    protected Explicit<Orientation> orientation;

    protected AbstractShapeGeometryFieldMapper(String simpleName, FieldType fieldType, MappedFieldType mappedFieldType,
                                               Explicit<Boolean> ignoreMalformed, Explicit<Boolean> coerce,
                                               Explicit<Boolean> ignoreZValue, Explicit<Orientation> orientation,
                                               MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, mappedFieldType, ignoreMalformed, ignoreZValue, multiFields, copyTo);
        this.coerce = coerce;
        this.orientation = orientation;
    }

    @Override
    public final boolean parsesArrayValue() {
        return false;
    }

    @Override
    protected final void mergeOptions(FieldMapper other, List<String> conflicts) {
        AbstractShapeGeometryFieldMapper gsfm = (AbstractShapeGeometryFieldMapper)other;
        if (gsfm.coerce.explicit()) {
            this.coerce = gsfm.coerce;
        }
        if (gsfm.orientation.explicit()) {
            this.orientation = gsfm.orientation;
        }
        mergeGeoOptions(gsfm, conflicts);
    }

    protected abstract void mergeGeoOptions(AbstractShapeGeometryFieldMapper<?,?> mergeWith, List<String> conflicts);

    @Override
    public void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
        AbstractShapeGeometryFieldType ft = (AbstractShapeGeometryFieldType)fieldType();
        if (includeDefaults || coerce.explicit()) {
            builder.field(AbstractShapeGeometryFieldMapper.Names.COERCE.getPreferredName(), coerce.value());
        }
        if (includeDefaults || orientation.explicit()) {
            builder.field(Names.ORIENTATION.getPreferredName(), orientation.value());
        }
    }

    public Explicit<Boolean> coerce() {
        return coerce;
    }

    public Orientation orientation() {
        return ((AbstractShapeGeometryFieldType)mappedFieldType).orientation();
    }
}
