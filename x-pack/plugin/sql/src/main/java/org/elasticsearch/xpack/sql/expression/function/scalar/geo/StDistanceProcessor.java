/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.geo;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.ql.expression.gen.processor.BinaryProcessor;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.literal.geo.GeoShape;

import java.io.IOException;
import java.util.Objects;

public class StDistanceProcessor extends BinaryProcessor {

    public static final String NAME = "geo_distance";

    public StDistanceProcessor(Processor source1, Processor source2) {
        super(source1, source2);
    }

    public StDistanceProcessor(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected void doWrite(StreamOutput out) throws IOException {

    }

    @Override
    public Object process(Object input) {
        Object l = left().process(input);
        checkParameter(l);
        Object r = right().process(input);
        checkParameter(r);
        return doProcess(l, r);
    }

    @Override
    protected Object doProcess(Object left, Object right) {
        return process(left, right);
    }

    public static Double process(Object source1, Object source2) {
        if (source1 == null || source2 == null) {
            return null;
        }

        if (source1 instanceof GeoShape == false) {
            throw new SqlIllegalArgumentException("A geo_point or geo_shape with type point is required; received [{}]", source1);
        }
        if (source2 instanceof GeoShape == false) {
            throw new SqlIllegalArgumentException("A geo_point or geo_shape with type point is required; received [{}]", source2);
        }
        return GeoShape.distance((GeoShape) source1, (GeoShape) source2);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        StDistanceProcessor other = (StDistanceProcessor) obj;
        return Objects.equals(left(), other.left())
            && Objects.equals(right(), other.right());
    }

    @Override
    public int hashCode() {
        return Objects.hash(left(), right());
    }
}
