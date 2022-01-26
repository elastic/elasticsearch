/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.string;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.util.Check;

import java.io.IOException;
import java.util.Objects;

public class SubstringFunctionProcessor implements Processor {

    public static final String NAME = "ssub";

    private final Processor input, start, length;

    public SubstringFunctionProcessor(Processor input, Processor start, Processor length) {
        this.input = input;
        this.start = start;
        this.length = length;
    }

    public SubstringFunctionProcessor(StreamInput in) throws IOException {
        input = in.readNamedWriteable(Processor.class);
        start = in.readNamedWriteable(Processor.class);
        length = in.readNamedWriteable(Processor.class);
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(input);
        out.writeNamedWriteable(start);
        out.writeNamedWriteable(length);
    }

    @Override
    public Object process(Object o) {
        return doProcess(input.process(o), start.process(o), length.process(o));
    }

    public static Object doProcess(Object input, Object start, Object length) {
        if (input == null) {
            return null;
        }
        if ((input instanceof String || input instanceof Character) == false) {
            throw new SqlIllegalArgumentException("A string/char is required; received [{}]", input);
        }
        if (start == null || length == null) {
            return null;
        }

        Check.isFixedNumberAndInRange(start, "start", (long) Integer.MIN_VALUE + 1, (long) Integer.MAX_VALUE);
        Check.isFixedNumberAndInRange(length, "length", 0L, (long) Integer.MAX_VALUE);

        return StringFunctionUtils.substring(
            input instanceof Character ? input.toString() : (String) input,
            ((Number) start).intValue() - 1, // SQL is 1-based when it comes to string manipulation
            ((Number) length).intValue()
        );
    }

    protected Processor input() {
        return input;
    }

    protected Processor start() {
        return start;
    }

    protected Processor length() {
        return length;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        SubstringFunctionProcessor other = (SubstringFunctionProcessor) obj;
        return Objects.equals(input(), other.input()) && Objects.equals(start(), other.start()) && Objects.equals(length(), other.length());
    }

    @Override
    public int hashCode() {
        return Objects.hash(input(), start(), length());
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
