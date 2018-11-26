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

public class LocateFunctionProcessor implements Processor {

    private final Processor pattern, source, start;
    public static final String NAME = "sloc";

    public LocateFunctionProcessor(Processor pattern, Processor source, Processor start) {
        this.pattern = pattern;
        this.source = source;
        this.start = start;
    }

    public LocateFunctionProcessor(StreamInput in) throws IOException {
        pattern = in.readNamedWriteable(Processor.class);
        source = in.readNamedWriteable(Processor.class);
        start = in.readOptionalNamedWriteable(Processor.class);
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(pattern);
        out.writeNamedWriteable(source);
        out.writeOptionalNamedWriteable(start);
    }

    @Override
    public Object process(Object input) {
        return doProcess(pattern().process(input), source().process(input), start() == null ? null : start().process(input));
    }

    public static Integer doProcess(Object pattern, Object source, Object start) {
        if (source == null) {
            return null;
        }
        if (!(source instanceof String || source instanceof Character)) {
            throw new SqlIllegalArgumentException("A string/char is required; received [{}]", source);
        }
        if (pattern == null) {
            return 0;
        }
        
        if (!(pattern instanceof String || pattern instanceof Character)) {
            throw new SqlIllegalArgumentException("A string/char is required; received [{}]", pattern);
        }
        if (start != null && !(start instanceof Number)) {
            throw new SqlIllegalArgumentException("A number is required; received [{}]", start);
        }
        
        String stringSource = source instanceof Character ? source.toString() : (String) source;
        String stringPattern = pattern instanceof Character ? pattern.toString() : (String) pattern;

        return Integer.valueOf(1 + (start != null ? 
                stringSource.indexOf(stringPattern, ((Number) start).intValue() - 1)
                : stringSource.indexOf(stringPattern)));
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        
        LocateFunctionProcessor other = (LocateFunctionProcessor) obj;
        return Objects.equals(pattern(), other.pattern())
                && Objects.equals(source(), other.source())
                && Objects.equals(start(), other.start());
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(pattern(), source(), start());
    }
    
    public Processor pattern() {
        return pattern;
    }
    
    public Processor source() {
        return source;
    }
    
    public Processor start() {
        return start;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
