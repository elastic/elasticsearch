/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql.expression.function.scalar.math;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.eql.EqlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;

import java.io.IOException;
import java.util.Objects;

public class ToNumberFunctionProcessor implements Processor {

    public static final String NAME = "num";

    private final Processor value, base;

    public ToNumberFunctionProcessor(Processor value, Processor base) {
        this.value = value;
        this.base = base;
    }

    public ToNumberFunctionProcessor(StreamInput in) throws IOException {
        value = in.readNamedWriteable(Processor.class);
        base = in.readNamedWriteable(Processor.class);
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(value);
        out.writeNamedWriteable(base);
    }

    @Override
    public Object process(Object input) {
        return doProcess(value.process(input), base.process(input));
    }

    private static Number parseDecimal(String source) {
        try {
            return Long.valueOf(source);
        } catch (NumberFormatException e) {
            return Double.valueOf(source);
        }
    }

    public static Object doProcess(Object value, Object base) {
        if (value == null) {
            return null;
        }

        if ((value instanceof String || value instanceof Character) == false) {
            throw new EqlIllegalArgumentException("A string/char is required; received [{}]", value);
        }

        boolean detectedHexPrefix = value.toString().startsWith("0x");

        if (base == null) {
            base = detectedHexPrefix ? 16 : 10;
        } else if (base instanceof Integer == false) {
            throw new EqlIllegalArgumentException("An integer base is required; received [{}]", base);
        }

        int radix = (Integer) base;

        if (detectedHexPrefix && radix == 16) {
            value = value.toString().substring(2);
        }

        try {
            if (radix == 10) {
                return parseDecimal(value.toString());
            } else {
                return Long.parseLong(value.toString(), radix);
            }
        } catch (NumberFormatException e) {
            throw new EqlIllegalArgumentException("Unable to convert [{}] to number of base [{}]", value, radix);
        }

    }

    protected Processor value() {
        return value;
    }

    protected Processor base() {
        return base;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ToNumberFunctionProcessor other = (ToNumberFunctionProcessor) obj;
        return Objects.equals(value(), other.value()) && Objects.equals(base(), other.base());
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, base);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
