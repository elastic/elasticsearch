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

    private final Processor input, pattern, replacement;
    public static final String NAME = "srep";

    public ReplaceFunctionProcessor(Processor input, Processor pattern, Processor replacement) {
        this.input = input;
        this.pattern = pattern;
        this.replacement = replacement;
    }

    public ReplaceFunctionProcessor(StreamInput in) throws IOException {
        input = in.readNamedWriteable(Processor.class);
        pattern = in.readNamedWriteable(Processor.class);
        replacement = in.readNamedWriteable(Processor.class);
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(input);
        out.writeNamedWriteable(pattern);
        out.writeNamedWriteable(replacement);
    }

    @Override
    public Object process(Object input) {
        return doProcess(input().process(input), pattern().process(input), replacement().process(input));
    }

    public static Object doProcess(Object input, Object pattern, Object replacement) {
        if (input == null) {
            return null;
        }
        if (!(input instanceof String || input instanceof Character)) {
            throw new SqlIllegalArgumentException("A string/char is required; received [{}]", input);
        }
        if (pattern == null || replacement == null) {
            return input;
        }
        if (!(pattern instanceof String || pattern instanceof Character)) {
            throw new SqlIllegalArgumentException("A string/char is required; received [{}]", pattern);
        }
        if (!(replacement instanceof String || replacement instanceof Character)) {
            throw new SqlIllegalArgumentException("A string/char is required; received [{}]", replacement);
        }
        
        return Strings.replace(input instanceof Character ? input.toString() : (String) input,
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
        return Objects.equals(input(), other.input())
                && Objects.equals(pattern(), other.pattern())
                && Objects.equals(replacement(), other.replacement());
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(input(), pattern(), replacement());
    }
    
    public Processor input() {
        return input;
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
