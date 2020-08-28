/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.string;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;

import java.io.IOException;
import java.util.Objects;

public class InsertFunctionProcessor implements Processor {

    private final Processor input, start, length, replacement;
    public static final String NAME = "si";

    public InsertFunctionProcessor(Processor input, Processor start, Processor length, Processor replacement) {
        this.input = input;
        this.start = start;
        this.length = length;
        this.replacement = replacement;
    }

    public InsertFunctionProcessor(StreamInput in) throws IOException {
        input = in.readNamedWriteable(Processor.class);
        start = in.readNamedWriteable(Processor.class);
        length = in.readNamedWriteable(Processor.class);
        replacement = in.readNamedWriteable(Processor.class);
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(input());
        out.writeNamedWriteable(start());
        out.writeNamedWriteable(length());
        out.writeNamedWriteable(replacement());
    }

    @Override
    public Object process(Object input) {
        return doProcess(input().process(input), start().process(input), length().process(input), replacement().process(input));
    }

    public static Object doProcess(Object input, Object start, Object length, Object replacement) {
        if (input == null) {
            return null;
        }
        if (!(input instanceof String || input instanceof Character)) {
            throw new SqlIllegalArgumentException("A string/char is required; received [{}]", input);
        }
        if (replacement == null) {
            return input;
        }
        if (!(replacement instanceof String || replacement instanceof Character)) {
            throw new SqlIllegalArgumentException("A string/char is required; received [{}]", replacement);
        }
        if (start == null || length == null) {
            return input;
        }
        if (!(start instanceof Number)) {
            throw new SqlIllegalArgumentException("A number is required; received [{}]", start);
        }
        if (!(length instanceof Number)) {
            throw new SqlIllegalArgumentException("A number is required; received [{}]", length);
        }
        if (((Number) length).intValue() < 0) {
            throw new SqlIllegalArgumentException("A positive number is required for [length]; received [{}]", length);
        }

        int startInt = ((Number) start).intValue() - 1;
        int realStart = startInt < 0 ? 0 : startInt;
        
        if (startInt > input.toString().length()) {
            return input;
        }
        
        StringBuilder sb = new StringBuilder(input.toString());
        String replString = (replacement.toString());

        return sb.replace(realStart,
                realStart + ((Number) length).intValue(),
                replString).toString();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        
        InsertFunctionProcessor other = (InsertFunctionProcessor) obj;
        return Objects.equals(input(), other.input())
                && Objects.equals(start(), other.start())
                && Objects.equals(length(), other.length())
                && Objects.equals(replacement(), other.replacement());
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(input(), start(), length(), replacement());
    }
    
    public Processor input() {
        return input;
    }
    
    public Processor start() {
        return start;
    }
    
    public Processor length() {
        return length;
    }
    
    public Processor replacement() {
        return replacement;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
