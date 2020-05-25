/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.expression.function.scalar.string;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.eql.EqlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

public class EndsWithFunctionProcessor implements Processor {

    public static final String NAME = "senw";

    private final Processor source;
    private final Processor pattern;

    public EndsWithFunctionProcessor(Processor source, Processor pattern) {
        this.source = source;
        this.pattern = pattern;
    }

    public EndsWithFunctionProcessor(StreamInput in) throws IOException {
        source = in.readNamedWriteable(Processor.class);
        pattern = in.readNamedWriteable(Processor.class);
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(source);
        out.writeNamedWriteable(pattern);
    }

    @Override
    public Object process(Object input) {
        return doProcess(source.process(input), pattern.process(input));
    }

    public static Object doProcess(Object source, Object pattern) {
        if (source == null) {
            return null;
        }
        if (source instanceof String == false && source instanceof Character == false) {
            throw new EqlIllegalArgumentException("A string/char is required; received [{}]", source);
        }
        if (pattern == null) {
            return null;
        }
        if (pattern instanceof String == false && pattern instanceof Character == false) {
            throw new EqlIllegalArgumentException("A string/char is required; received [{}]", pattern);
        }

        return source.toString().toLowerCase(Locale.ROOT).endsWith(pattern.toString().toLowerCase(Locale.ROOT));
    }
    
    protected Processor source() {
        return source;
    }

    protected Processor pattern() {
        return pattern;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        
        EndsWithFunctionProcessor other = (EndsWithFunctionProcessor) obj;
        return Objects.equals(source(), other.source())
                && Objects.equals(pattern(), other.pattern());
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(source(), pattern());
    }
    

    @Override
    public String getWriteableName() {
        return NAME;
    }
}