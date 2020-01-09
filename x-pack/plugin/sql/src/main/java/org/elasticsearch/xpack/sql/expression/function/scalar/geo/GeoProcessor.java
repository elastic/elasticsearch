/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.geo;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.ql.expression.function.scalar.geo.GeoShape;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;

import java.io.IOException;
import java.util.function.Function;

public class GeoProcessor implements Processor {

    private interface GeoShapeFunction<R> {
        default R apply(Object o) {
            if (o instanceof GeoShape) {
                return doApply((GeoShape) o);
            } else {
                throw new SqlIllegalArgumentException("A geo_point or geo_shape is required; received [{}]", o);
            }
        }

        R doApply(GeoShape s);
    }

    public enum GeoOperation {
        ASWKT(GeoShape::toString),
        GEOMETRY_TYPE(GeoShape::getGeometryType),
        X(GeoShape::getX),
        Y(GeoShape::getY),
        Z(GeoShape::getZ);

        private final Function<Object, Object> apply;

        GeoOperation(GeoShapeFunction<Object> apply) {
            this.apply = l -> l == null ? null : apply.apply(l);
        }

        public final Object apply(Object l) {
            return apply.apply(l);
        }
    }

    public static final String NAME = "geo";

    private final GeoOperation processor;

    public GeoProcessor(GeoOperation processor) {
        this.processor = processor;
    }

    public GeoProcessor(StreamInput in) throws IOException {
        processor = in.readEnum(GeoOperation.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(processor);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Object process(Object input) {
        return processor.apply(input);
    }

    GeoOperation processor() {
        return processor;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        GeoProcessor other = (GeoProcessor) obj;
        return processor == other.processor;
    }

    @Override
    public int hashCode() {
        return processor.hashCode();
    }

    @Override
    public String toString() {
        return processor.toString();
    }
}
