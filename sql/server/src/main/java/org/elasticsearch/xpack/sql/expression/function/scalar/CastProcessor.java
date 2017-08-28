/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.sql.type.DataTypeConversion.Conversion;

import java.io.IOException;

public class CastProcessor implements ColumnProcessor {
    public static final String NAME = "c";
    private final Conversion conversion;

    CastProcessor(Conversion conversion) {
        this.conversion = conversion;
    }

    CastProcessor(StreamInput in) throws IOException {
        conversion = in.readEnum(Conversion.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(conversion);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Object apply(Object r) {
        return conversion.convert(r);
    }

    Conversion converter() {
        return conversion;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        CastProcessor other = (CastProcessor) obj;
        return conversion.equals(other.conversion);
    }

    @Override
    public int hashCode() {
        return conversion.hashCode();
    }

    @Override
    public String toString() {
        return conversion.toString();
    }
}
