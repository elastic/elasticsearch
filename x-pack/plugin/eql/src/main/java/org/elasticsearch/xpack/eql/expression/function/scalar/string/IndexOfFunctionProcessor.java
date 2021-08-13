/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
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

    private final Processor input;
    private final Processor substring;
    private final Processor start;
    private final boolean caseInsensitive;

    public IndexOfFunctionProcessor(Processor input, Processor substring, Processor start, boolean caseInsensitive) {
        this.input = input;
        this.substring = substring;
        this.start = start;
        this.caseInsensitive = caseInsensitive;
    }

    public IndexOfFunctionProcessor(StreamInput in) throws IOException {
        input = in.readNamedWriteable(Processor.class);
        substring = in.readNamedWriteable(Processor.class);
        start = in.readNamedWriteable(Processor.class);
        caseInsensitive = in.readBoolean();
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(input);
        out.writeNamedWriteable(substring);
        out.writeNamedWriteable(start);
        out.writeBoolean(caseInsensitive);
    }

    @Override
    public Object process(Object o) {
        return doProcess(input.process(o), substring.process(o), start.process(o), isCaseInsensitive());
    }

    public static Object doProcess(Object input, Object substring, Object start, boolean caseInsensitive) {
        if (input == null) {
            return null;
        }
        if (input instanceof String == false && input instanceof Character == false) {
            throw new EqlIllegalArgumentException("A string/char is required; received [{}]", input);
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

        int result;

        if (caseInsensitive == false) {
            result = input.toString().indexOf(substring.toString(), startIndex);
        } else {
            result = input.toString().toLowerCase(Locale.ROOT).indexOf(substring.toString().toLowerCase(Locale.ROOT), startIndex);
        }

        return result < 0 ? null : result;
    }

    protected Processor input() {
        return input;
    }

    protected Processor substring() {
        return substring;
    }

    protected Processor start() {
        return start;
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

        IndexOfFunctionProcessor other = (IndexOfFunctionProcessor) obj;
        return Objects.equals(input(), other.input())
            && Objects.equals(substring(), other.substring())
            && Objects.equals(start(), other.start())
            && Objects.equals(isCaseInsensitive(), other.isCaseInsensitive());
    }

    @Override
    public int hashCode() {
        return Objects.hash(input(), substring(), start(), isCaseInsensitive());
    }


    @Override
    public String getWriteableName() {
        return NAME;
    }
}
