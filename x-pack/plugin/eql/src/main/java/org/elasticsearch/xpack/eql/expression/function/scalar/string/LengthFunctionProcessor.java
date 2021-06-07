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

public class LengthFunctionProcessor implements Processor {

    public static final String NAME = "slen";

    private final Processor input;

    public LengthFunctionProcessor(Processor input) {
        this.input = input;
    }

    public LengthFunctionProcessor(StreamInput in) throws IOException {
        input = in.readNamedWriteable(Processor.class);
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(input);
    }

    @Override
    public Object process(Object o) {
        return doProcess(input.process(o));
    }

    public static Object doProcess(Object input) {
        if (input == null) {
            return null;
        }
        if (input instanceof String == false && input instanceof Character == false) {
            throw new EqlIllegalArgumentException("A string/char is required; received [{}]", input);
        }

        return input.toString().length();
    }

    protected Processor input() {
        return input;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        return Objects.equals(input(), ((LengthFunctionProcessor) obj).input());
    }

    @Override
    public int hashCode() {
        return Objects.hash(input());
    }


    @Override
    public String getWriteableName() {
        return NAME;
    }
}
