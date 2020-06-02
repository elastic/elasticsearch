/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.ql.type.Converter;

import java.io.IOException;
import java.util.Objects;

public class CastProcessor implements Processor {

    public static final String NAME = "ca";

    private final Converter conversion;

    public CastProcessor(Converter conversion) {
        this.conversion = conversion;
    }

    public CastProcessor(StreamInput in) throws IOException {
        conversion = in.readNamedWriteable(Converter.class);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(conversion);
    }

    @Override
    public Object process(Object input) {
        return conversion.convert(input);
    }

    Converter converter() {
        return conversion;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        CastProcessor other = (CastProcessor) obj;
        return Objects.equals(conversion, other.conversion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(conversion);
    }

    @Override
    public String toString() {
        return conversion.getClass().getSimpleName();
    }
}
