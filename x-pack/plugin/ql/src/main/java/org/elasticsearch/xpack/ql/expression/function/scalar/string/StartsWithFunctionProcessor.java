/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.expression.function.scalar.string;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

public class StartsWithFunctionProcessor implements Processor {

    public static final String NAME = "sstw";

    private final Processor source;
    private final Processor pattern;
    private final boolean caseInsensitive;

    public StartsWithFunctionProcessor(Processor source, Processor pattern, boolean caseInsensitive) {
        this.source = source;
        this.pattern = pattern;
        this.caseInsensitive = caseInsensitive;
    }

    public StartsWithFunctionProcessor(StreamInput in) throws IOException {
        source = in.readNamedWriteable(Processor.class);
        pattern = in.readNamedWriteable(Processor.class);
        caseInsensitive = in.readBoolean();
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(source);
        out.writeNamedWriteable(pattern);
        out.writeBoolean(caseInsensitive);
    }

    @Override
    public Object process(Object input) {
        return doProcess(source.process(input), pattern.process(input), isCaseInsensitive());
    }

    public static Object doProcess(Object source, Object pattern, boolean caseInsensitive) {
        if (source == null) {
            return null;
        }
        if (source instanceof String == false && source instanceof Character == false) {
            throw new QlIllegalArgumentException("A string/char is required; received [{}]", source);
        }
        if (pattern == null) {
            return null;
        }
        if (pattern instanceof String == false && pattern instanceof Character == false) {
            throw new QlIllegalArgumentException("A string/char is required; received [{}]", pattern);
        }

        if (caseInsensitive == false) {
            return source.toString().startsWith(pattern.toString());
        } else {
            return source.toString().toLowerCase(Locale.ROOT).startsWith(pattern.toString().toLowerCase(Locale.ROOT));
        }
    }

    protected Processor source() {
        return source;
    }

    protected Processor pattern() {
        return pattern;
    }

    protected boolean isCaseInsensitive() {
        return caseInsensitive;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        StartsWithFunctionProcessor other = (StartsWithFunctionProcessor) obj;
        return Objects.equals(source(), other.source())
            && Objects.equals(pattern(), other.pattern())
            && Objects.equals(isCaseInsensitive(), other.isCaseInsensitive());
    }

    @Override
    public int hashCode() {
        return Objects.hash(source(), pattern(), isCaseInsensitive());
    }


    @Override
    public String getWriteableName() {
        return NAME;
    }
}
