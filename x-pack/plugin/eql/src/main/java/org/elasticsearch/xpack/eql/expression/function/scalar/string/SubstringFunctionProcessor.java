/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql.expression.function.scalar.string;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.eql.EqlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;

import java.io.IOException;
import java.util.Objects;

public class SubstringFunctionProcessor implements Processor {

    public static final String NAME = "ssub";

    private final Processor input, start, end;

    public SubstringFunctionProcessor(Processor source, Processor start, Processor end) {
        this.input = source;
        this.start = start;
        this.end = end;
    }

    public SubstringFunctionProcessor(StreamInput in) throws IOException {
        input = in.readNamedWriteable(Processor.class);
        start = in.readNamedWriteable(Processor.class);
        end = in.readNamedWriteable(Processor.class);
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(input);
        out.writeNamedWriteable(start);
        out.writeNamedWriteable(end);
    }

    @Override
    public Object process(Object o) {
        return doProcess(input.process(o), start.process(o), end.process(o));
    }

    public static Object doProcess(Object input, Object start, Object end) {
        if (input == null) {
            return null;
        }
        if ((input instanceof String || input instanceof Character) == false) {
            throw new EqlIllegalArgumentException("A string/char is required; received [{}]", input);
        }
        if (start == null) {
            return input;
        }
        if ((start instanceof Number) == false) {
            throw new EqlIllegalArgumentException("A number is required; received [{}]", start);
        }
        if (end != null && (end instanceof Number) == false) {
            throw new EqlIllegalArgumentException("A number is required; received [{}]", end);
        }

        String str = input.toString();
        int startIndex = ((Number) start).intValue();
        int endIndex = end == null ? str.length() : ((Number) end).intValue();

        return StringUtils.substringSlice(str, startIndex, endIndex);
    }

    protected Processor input() {
        return input;
    }

    protected Processor start() {
        return start;
    }

    protected Processor end() {
        return end;
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
        return Objects.equals(input(), other.input())
                && Objects.equals(start(), other.start())
                && Objects.equals(end(), other.end());
    }

    @Override
    public int hashCode() {
        return Objects.hash(input(), start(), end());
    }


    @Override
    public String getWriteableName() {
        return NAME;
    }
}
