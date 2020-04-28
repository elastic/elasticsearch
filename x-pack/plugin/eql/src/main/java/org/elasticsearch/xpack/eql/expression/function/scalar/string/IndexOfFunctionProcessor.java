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

public class IndexOfFunctionProcessor implements Processor {

    public static final String NAME = "siof";

    private final Processor source;
    private final Processor substring;
    private final Processor start;

    public IndexOfFunctionProcessor(Processor source, Processor substring, Processor start) {
        this.source = source;
        this.substring = substring;
        this.start = start;
    }

    public IndexOfFunctionProcessor(StreamInput in) throws IOException {
        source = in.readNamedWriteable(Processor.class);
        substring = in.readNamedWriteable(Processor.class);
        start = in.readNamedWriteable(Processor.class);
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(source);
        out.writeNamedWriteable(substring);
        out.writeNamedWriteable(start);
    }

    @Override
    public Object process(Object input) {
        return doProcess(source.process(input), substring.process(input), start.process(input));
    }

    public static Object doProcess(Object source, Object substring, Object start) {
        if (source == null) {
            return null;
        }
        if (source instanceof String == false && source instanceof Character == false) {
            throw new EqlIllegalArgumentException("A string/char is required; received [{}]", source);
        }
        if (substring == null) {
            return null;
        }
        if (substring instanceof String == false && substring instanceof Character == false) {
            throw new EqlIllegalArgumentException("A string/char is required; received [{}]", substring);
        }
        
        if (start != null && start instanceof Number == false) {
            throw new EqlIllegalArgumentException("A number is required; received [{}]", start);
        }
        int startIndex = start == null ? 0 : ((Number) start).intValue();
        int result = source.toString().toLowerCase(Locale.ROOT).indexOf(substring.toString().toLowerCase(Locale.ROOT), startIndex);

        return result < 0 ? null : result;
    }
    
    protected Processor source() {
        return source;
    }

    protected Processor substring() {
        return substring;
    }

    protected Processor start() {
        return start;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        
        IndexOfFunctionProcessor other = (IndexOfFunctionProcessor) obj;
        return Objects.equals(source(), other.source())
                && Objects.equals(substring(), other.substring())
                && Objects.equals(start(), other.start());
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(source(), substring(), start());
    }
    

    @Override
    public String getWriteableName() {
        return NAME;
    }
}