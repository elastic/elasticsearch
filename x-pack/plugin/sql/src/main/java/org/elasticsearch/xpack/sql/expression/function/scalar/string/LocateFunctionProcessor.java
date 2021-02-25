/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.string;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.util.Check;

import java.io.IOException;
import java.util.Objects;

public class LocateFunctionProcessor implements Processor {

    private final Processor pattern, input, start;
    public static final String NAME = "sloc";

    public LocateFunctionProcessor(Processor pattern, Processor input, Processor start) {
        this.pattern = pattern;
        this.input = input;
        this.start = start;
    }

    public LocateFunctionProcessor(StreamInput in) throws IOException {
        pattern = in.readNamedWriteable(Processor.class);
        input = in.readNamedWriteable(Processor.class);
        start = in.readOptionalNamedWriteable(Processor.class);
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(pattern);
        out.writeNamedWriteable(input);
        out.writeOptionalNamedWriteable(start);
    }

    @Override
    public Object process(Object input) {
        return doProcess(pattern().process(input), input().process(input), start() == null ? null : start().process(input));
    }
    
    public static Integer doProcess(Object pattern, Object input, Object start) {
        if (pattern == null || input == null) {
            return null;
        }
        if ((input instanceof String || input instanceof Character) == false) {
            throw new SqlIllegalArgumentException("A string/char is required; received [{}]", input);
        }

        if ((pattern instanceof String || pattern instanceof Character) == false) {
            throw new SqlIllegalArgumentException("A string/char is required; received [{}]", pattern);
        }

        if (start != null) {
            Check.isFixedNumberAndInRange(start, "start", (long) Integer.MIN_VALUE + 1, (long) Integer.MAX_VALUE);
        }

        String stringInput = input instanceof Character ? input.toString() : (String) input;
        String stringPattern = pattern instanceof Character ? pattern.toString() : (String) pattern;

        
        int startIndex = start == null ? 0 : ((Number) start).intValue() - 1;
        return 1 + stringInput.indexOf(stringPattern, startIndex);
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
                && Objects.equals(input(), other.input())
                && Objects.equals(start(), other.start());
    }

    @Override
    public int hashCode() {
        return Objects.hash(pattern(), input(), start());
    }

    public Processor pattern() {
        return pattern;
    }

    public Processor input() {
        return input;
    }

    public Processor start() {
        return start;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
