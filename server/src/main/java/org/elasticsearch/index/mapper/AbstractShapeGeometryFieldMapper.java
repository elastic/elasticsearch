/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.mapper;

import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.geo.Orientation;

import java.util.Map;
import java.util.function.Function;

/**
 * Base class for shape field mappers
 */
public abstract class AbstractShapeGeometryFieldMapper<T> extends AbstractGeometryFieldMapper<T> {
    @Override
    protected boolean supportsParsingObject() {
        // ShapeGeometryFieldMapper supports parsing Well-Known Text (WKT) and GeoJSON.
        // WKT are of type String and GeoJSON for all shapes are of type Array.
        return false;
    }

    public static Parameter<Explicit<Boolean>> coerceParam(Function<FieldMapper, Explicit<Boolean>> initializer, boolean coerceByDefault) {
        return Parameter.explicitBoolParam("coerce", true, initializer, coerceByDefault);
    }

    private static final Explicit<Orientation> IMPLICIT_RIGHT = new Explicit<>(Orientation.RIGHT, false);

    public static Parameter<Explicit<Orientation>> orientationParam(Function<FieldMapper, Explicit<Orientation>> initializer) {
        return new Parameter<>(
            "orientation",
            true,
            () -> IMPLICIT_RIGHT,
            (n, c, o) -> new Explicit<>(Orientation.fromString(o.toString()), true),
            initializer,
            (b, f, v) -> b.field(f, v.value()),
            v -> v.value().toString()
        );
    }

    public abstract static class AbstractShapeGeometryFieldType<T> extends AbstractGeometryFieldType<T> {

        private final Orientation orientation;

        protected AbstractShapeGeometryFieldType(
            String name,
            boolean isSearchable,
            boolean isStored,
            boolean hasDocValues,
            Parser<T> parser,
            Orientation orientation,
            Map<String, String> meta
        ) {
            super(name, isSearchable, isStored, hasDocValues, parser, null, meta);
            this.orientation = orientation;
        }

        public Orientation orientation() {
            return this.orientation;
        }

        @Override
        protected Object nullValueAsSource(T nullValue) {
            // we don't support null value fors shapes
            return nullValue;
        }
    }

    protected Explicit<Boolean> coerce;
    protected Explicit<Orientation> orientation;

    protected AbstractShapeGeometryFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        Explicit<Boolean> ignoreMalformed,
        Explicit<Boolean> coerce,
        Explicit<Boolean> ignoreZValue,
        Explicit<Orientation> orientation,
        MultiFields multiFields,
        CopyTo copyTo,
        Parser<T> parser
    ) {
        super(simpleName, mappedFieldType, ignoreMalformed, ignoreZValue, multiFields, copyTo, parser);
        this.coerce = coerce;
        this.orientation = orientation;
    }

    protected AbstractShapeGeometryFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        MultiFields multiFields,
        Explicit<Boolean> coerce,
        Explicit<Orientation> orientation,
        CopyTo copyTo,
        Parser<T> parser,
        OnScriptError onScriptError
    ) {
        super(simpleName, mappedFieldType, multiFields, copyTo, parser, onScriptError);
        this.coerce = coerce;
        this.orientation = orientation;
    }

    public boolean coerce() {
        return coerce.value();
    }

    public Orientation orientation() {
        return orientation.value();
    }
}
