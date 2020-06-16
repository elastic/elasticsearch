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
import java.util.Objects;

public class SubstringFunctionProcessor implements Processor {

    public static final String NAME = "ssub";

    private final Processor source, start, end;

    public SubstringFunctionProcessor(Processor source, Processor start, Processor end) {
        this.source = source;
        this.start = start;
        this.end = end;
    }

    public SubstringFunctionProcessor(StreamInput in) throws IOException {
        source = in.readNamedWriteable(Processor.class);
        start = in.readNamedWriteable(Processor.class);
        end = in.readNamedWriteable(Processor.class);
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(source);
        out.writeNamedWriteable(start);
        out.writeNamedWriteable(end);
    }

    @Override
    public Object process(Object input) {
        return doProcess(source.process(input), start.process(input), end.process(input));
    }

    public static Object doProcess(Object source, Object start, Object end) {
        if (source == null) {
            return null;
        }
        if (!(source instanceof String || source instanceof Character)) {
            throw new EqlIllegalArgumentException("A string/char is required; received [{}]", source);
        }
        if (start == null) {
            return source;
        }
        if ((start instanceof Number) == false) {
            throw new EqlIllegalArgumentException("A number is required; received [{}]", start);
        }
        if (end != null && (end instanceof Number) == false) {
            throw new EqlIllegalArgumentException("A number is required; received [{}]", end);
        }

        String str = source.toString();
        int startIndex = ((Number) start).intValue();
        int endIndex = end == null ? str.length() : ((Number) end).intValue();
                
        return StringUtils.substringSlice(str, startIndex, endIndex);
    }
    
    protected Processor source() {
        return source;
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
        return Objects.equals(source(), other.source())
                && Objects.equals(start(), other.start())
                && Objects.equals(end(), other.end());
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(source(), start(), end());
    }
    

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
