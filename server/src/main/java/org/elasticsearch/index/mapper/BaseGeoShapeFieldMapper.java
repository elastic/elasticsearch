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

import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.geo.builders.ShapeBuilder.Orientation;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.LegacyGeoShapeFieldMapper.DeprecatedParameters;

import java.util.Map;
import java.util.Objects;

/**
 * Base class for {@link GeoShapeFieldMapper} and {@link LegacyGeoShapeFieldMapper}
 */
public abstract class BaseGeoShapeFieldMapper extends BaseGeometryFieldMapper {
    public static final String CONTENT_TYPE = "geo_shape";

    public abstract static class Builder<T extends Builder, Y extends BaseGeoShapeFieldMapper>
            extends BaseGeometryFieldMapper.Builder<T, Y> {

        /** default builder - used for external mapper*/
        public Builder(String name, MappedFieldType fieldType, MappedFieldType defaultFieldType) {
            super(name, fieldType, defaultFieldType);
        }

        public Builder(String name, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                       boolean coerce, boolean ignoreMalformed, Orientation orientation, boolean ignoreZ) {
            super(name, fieldType, defaultFieldType, coerce, ignoreMalformed, orientation, ignoreZ);
        }


        @Override
        protected void setupFieldType(BuilderContext context) {
            // field mapper handles this at build time
            // but prefix tree strategies require a name, so throw a similar exception
            if (name().isEmpty()) {
                throw new IllegalArgumentException("name cannot be empty string");
            }
            super.setupFieldType(context);
        }
    }

    protected static String DEPRECATED_PARAMETERS_KEY = "deprecatedParameters";

    public static class TypeParser extends BaseGeometryFieldMapper.TypeParser {

        @Override
        protected boolean parseXContentParameters(String name, Map.Entry<String, Object> entry,
                                                  Map<String, Object> params) throws MapperParsingException {
            DeprecatedParameters deprecatedParameters = new DeprecatedParameters();
            if (DeprecatedParameters.parse(name, entry.getKey(), entry.getValue(), deprecatedParameters)) {
                params.put(DEPRECATED_PARAMETERS_KEY, deprecatedParameters);
                return true;
            }
            return false;
        }

        @Override
        public BaseGeometryFieldMapper.Builder newBuilder(String name, Map<String, Object> params) {
            final Builder builder;
            if (params.containsKey(DEPRECATED_PARAMETERS_KEY)) {
                // Legacy index-based shape
                builder = new LegacyGeoShapeFieldMapper.Builder(name, (DeprecatedParameters)params.get(DEPRECATED_PARAMETERS_KEY));
            } else {
                // BKD-based shape
                builder = new GeoShapeFieldMapper.Builder(name);
            }
            return builder;
        }
    }

    public abstract static class BaseGeoShapeFieldType extends BaseGeometryFieldType {

        protected BaseGeoShapeFieldType() {
            super();
        }

        protected BaseGeoShapeFieldType(BaseGeoShapeFieldType ref) {
            super(ref);
        }

        @Override
        public boolean equals(Object o) {
            if (!super.equals(o)) return false;
            return true;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode());
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }
    }

    protected BaseGeoShapeFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                                     Explicit<Boolean> ignoreMalformed, Explicit<Boolean> coerce,
                                     Explicit<Boolean> ignoreZValue, Settings indexSettings,
                                     MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, ignoreMalformed, coerce, ignoreZValue, indexSettings, multiFields, copyTo);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}
