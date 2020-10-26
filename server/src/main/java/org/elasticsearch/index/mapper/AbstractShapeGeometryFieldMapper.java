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
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilder.Orientation;

import java.util.Map;
import java.util.function.Function;

/**
 * Base class for {@link GeoShapeFieldMapper} and {@link LegacyGeoShapeFieldMapper}
 */
public abstract class AbstractShapeGeometryFieldMapper<Parsed, Processed> extends AbstractGeometryFieldMapper<Parsed, Processed> {

    public static Parameter<Explicit<Boolean>> coerceParam(Function<FieldMapper, Explicit<Boolean>> initializer,
                                                           boolean coerceByDefault) {
        return Parameter.explicitBoolParam("coerce", true, initializer, coerceByDefault);
    }

    public static Parameter<Explicit<Orientation>> orientationParam(Function<FieldMapper, Explicit<Orientation>> initializer) {
        return new Parameter<>("orientation", true,
            () -> new Explicit<>(Orientation.RIGHT, false),
            (n, c, o) -> new Explicit<>(ShapeBuilder.Orientation.fromString(o.toString()), true),
            initializer)
            .setSerializer((b, f, v) -> b.field(f, v.value()), v -> v.value().toString());
    }

    public abstract static class AbstractShapeGeometryFieldType extends AbstractGeometryFieldType {

        private final Orientation orientation;

        protected AbstractShapeGeometryFieldType(String name, boolean isSearchable, boolean isStored, boolean hasDocValues,
                                                 boolean parsesArrayValue, Parser<?> parser,
                                                 Orientation orientation, Map<String, String> meta) {
            super(name, isSearchable, isStored, hasDocValues, parsesArrayValue, parser, meta);
            this.orientation = orientation;
        }

        public Orientation orientation() { return this.orientation; }
    }

    protected Explicit<Boolean> coerce;
    protected Explicit<Orientation> orientation;

    protected AbstractShapeGeometryFieldMapper(String simpleName, MappedFieldType mappedFieldType,
                                               Explicit<Boolean> ignoreMalformed, Explicit<Boolean> coerce,
                                               Explicit<Boolean> ignoreZValue, Explicit<Orientation> orientation,
                                               MultiFields multiFields, CopyTo copyTo,
                                               Indexer<Parsed, Processed> indexer, Parser<Parsed> parser) {
        super(simpleName, mappedFieldType, ignoreMalformed, ignoreZValue, multiFields, copyTo, indexer, parser);
        this.coerce = coerce;
        this.orientation = orientation;
    }

    @Override
    public final boolean parsesArrayValue() {
        return false;
    }

    public boolean coerce() {
        return coerce.value();
    }

    public Orientation orientation() {
        return orientation.value();
    }
}
