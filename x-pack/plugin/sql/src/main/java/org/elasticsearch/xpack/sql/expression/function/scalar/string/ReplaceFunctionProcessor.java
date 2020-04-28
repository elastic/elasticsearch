/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.string;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;

import java.io.IOException;
import java.util.Objects;

public class ReplaceFunctionProcessor implements Processor {

    private final Processor source, pattern, replacement;
    public static final String NAME = "srep";

    public ReplaceFunctionProcessor(Processor source, Processor pattern, Processor replacement) {
        this.source = source;
        this.pattern = pattern;
        this.replacement = replacement;
    }

    public ReplaceFunctionProcessor(StreamInput in) throws IOException {
        source = in.readNamedWriteable(Processor.class);
        pattern = in.readNamedWriteable(Processor.class);
        replacement = in.readNamedWriteable(Processor.class);
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(source);
        out.writeNamedWriteable(pattern);
        out.writeNamedWriteable(replacement);
    }

    @Override
    public Object process(Object input) {
        return doProcess(source().process(input), pattern().process(input), replacement().process(input));
    }

    public static Object doProcess(Object source, Object pattern, Object replacement) {
        if (source == null) {
            return null;
        }
        if (!(source instanceof String || source instanceof Character)) {
            throw new SqlIllegalArgumentException("A string/char is required; received [{}]", source);
        }
        if (pattern == null || replacement == null) {
            return source;
        }
        if (!(pattern instanceof String || pattern instanceof Character)) {
            throw new SqlIllegalArgumentException("A string/char is required; received [{}]", pattern);
        }
        if (!(replacement instanceof String || replacement instanceof Character)) {
            throw new SqlIllegalArgumentException("A string/char is required; received [{}]", replacement);
        }
        
        return Strings.replace(source instanceof Character ? source.toString() : (String)source,
                pattern instanceof Character ? pattern.toString() : (String) pattern,
                replacement instanceof Character ? replacement.toString() : (String) replacement);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        
        ReplaceFunctionProcessor other = (ReplaceFunctionProcessor) obj;
        return Objects.equals(source(), other.source())
                && Objects.equals(pattern(), other.pattern())
                && Objects.equals(replacement(), other.replacement());
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(source(), pattern(), replacement());
    }
    
    public Processor source() {
        return source;
    }

    public Processor pattern() {
        return pattern;
    }
    
    public Processor replacement() {
        return replacement;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
