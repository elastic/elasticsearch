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

public class LengthFunctionProcessor implements Processor {

    public static final String NAME = "slen";

    private final Processor source;

    public LengthFunctionProcessor(Processor source) {
        this.source = source;
    }

    public LengthFunctionProcessor(StreamInput in) throws IOException {
        source = in.readNamedWriteable(Processor.class);
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(source);
    }

    @Override
    public Object process(Object input) {
        return doProcess(source.process(input));
    }

    public static Object doProcess(Object source) {
        if (source == null) {
            return null;
        }
        if (source instanceof String == false && source instanceof Character == false) {
            throw new EqlIllegalArgumentException("A string/char is required; received [{}]", source);
        }

        return source.toString().length();
    }
    
    protected Processor source() {
        return source;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        
        return Objects.equals(source(), ((LengthFunctionProcessor) obj).source());
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(source());
    }
    

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
