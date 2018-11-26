/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.string;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.gen.processor.Processor;

import java.io.IOException;
import java.util.Objects;

public class SubstringFunctionProcessor implements Processor {

    public static final String NAME = "ssub";

    private final Processor source, start, length;

    public SubstringFunctionProcessor(Processor source, Processor start, Processor length) {
        this.source = source;
        this.start = start;
        this.length = length;
    }

    public SubstringFunctionProcessor(StreamInput in) throws IOException {
        source = in.readNamedWriteable(Processor.class);
        start = in.readNamedWriteable(Processor.class);
        length = in.readNamedWriteable(Processor.class);
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(source);
        out.writeNamedWriteable(start);
        out.writeNamedWriteable(length);
    }

    @Override
    public Object process(Object input) {
        return doProcess(source.process(input), start.process(input), length.process(input));
    }

    public static Object doProcess(Object source, Object start, Object length) {
        if (source == null) {
            return null;
        }
        if (!(source instanceof String || source instanceof Character)) {
            throw new SqlIllegalArgumentException("A string/char is required; received [{}]", source);
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

        return StringFunctionUtils.substring(source instanceof Character ? source.toString() : (String) source,
                ((Number) start).intValue() - 1, // SQL is 1-based when it comes to string manipulation
                ((Number) length).intValue());
    }
    
    protected Processor source() {
        return source;
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
        return Objects.equals(source(), other.source())
                && Objects.equals(start(), other.start())
                && Objects.equals(length(), other.length());
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(source(), start(), length());
    }
    

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
