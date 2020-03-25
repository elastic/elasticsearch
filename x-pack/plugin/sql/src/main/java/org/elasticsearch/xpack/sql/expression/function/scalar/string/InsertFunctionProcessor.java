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

    private final Processor source, start, length, replacement;
    public static final String NAME = "si";

    public InsertFunctionProcessor(Processor source, Processor start, Processor length, Processor replacement) {
        this.source = source;
        this.start = start;
        this.length = length;
        this.replacement = replacement;
    }

    public InsertFunctionProcessor(StreamInput in) throws IOException {
        source = in.readNamedWriteable(Processor.class);
        start = in.readNamedWriteable(Processor.class);
        length = in.readNamedWriteable(Processor.class);
        replacement = in.readNamedWriteable(Processor.class);
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(source());
        out.writeNamedWriteable(start());
        out.writeNamedWriteable(length());
        out.writeNamedWriteable(replacement());
    }

    @Override
    public Object process(Object input) {
        return doProcess(source().process(input), start().process(input), length().process(input), replacement().process(input));
    }

    public static Object doProcess(Object source, Object start, Object length, Object replacement) {
        if (source == null) {
            return null;
        }
        if (!(source instanceof String || source instanceof Character)) {
            throw new SqlIllegalArgumentException("A string/char is required; received [{}]", source);
        }
        if (replacement == null) {
            return source;
        }
        if (!(replacement instanceof String || replacement instanceof Character)) {
            throw new SqlIllegalArgumentException("A string/char is required; received [{}]", replacement);
        }
        if (start == null || length == null) {
            return source;
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
        
        if (startInt > source.toString().length()) {
            return source;
        }
        
        StringBuilder sb = new StringBuilder(source.toString());
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
        return Objects.equals(source(), other.source())
                && Objects.equals(start(), other.start())
                && Objects.equals(length(), other.length())
                && Objects.equals(replacement(), other.replacement());
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(source(), start(), length(), replacement());
    }
    
    public Processor source() {
        return source;
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
