/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.expression.function.scalar.stringcontains;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.eql.EqlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;

import java.io.IOException;
import java.util.Objects;

public class StringContainsFunctionProcessor implements Processor {

    public static final String NAME = "sstringcontains";

    private final Processor haystack, needle, caseSensitive;

    public StringContainsFunctionProcessor(Processor haystack, Processor needle, Processor caseSensitive) {
        this.haystack = haystack;
        this.needle = needle;
        this.caseSensitive = caseSensitive;
    }

    public StringContainsFunctionProcessor(StreamInput in) throws IOException {
        haystack = in.readNamedWriteable(Processor.class);
        needle = in.readNamedWriteable(Processor.class);
        caseSensitive = in.readNamedWriteable(Processor.class);
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(haystack);
        out.writeNamedWriteable(needle);
        out.writeNamedWriteable(caseSensitive);
    }

    @Override
    public Object process(Object input) {
        return doProcess(haystack.process(input), needle.process(input), caseSensitive.process(input));
    }

    public static Object doProcess(Object haystack, Object needle, Object caseSensitive) {
        if (haystack == null) {
            return null;
        }

        throwIfNotString(haystack);
        throwIfNotString(needle);

        throwIfNotBoolean(caseSensitive);

        String strHaystack = haystack.toString();
        String strNeedle = needle.toString();
        boolean bCaseSensitive = ((Boolean) caseSensitive).booleanValue();
        return StringContainsUtils.stringContains(strHaystack, strNeedle, bCaseSensitive);
    }

    private static void throwIfNotString(Object obj) {
        if (!(obj instanceof String || obj instanceof Character)) {
            throw new EqlIllegalArgumentException("A string/char is required; received [{}]", obj);
        }
    }

    private static void throwIfNotBoolean(Object obj) {
        if (!(obj instanceof Boolean)) {
            throw new EqlIllegalArgumentException("A boolean is required; received [{}]", obj);
        }
    }

    protected Processor haystack() {
        return haystack;
    }

    public Processor needle() {
        return needle;
    }

    public Processor caseSensitive() {
        return caseSensitive;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        StringContainsFunctionProcessor other = (StringContainsFunctionProcessor) obj;
        return Objects.equals(haystack(), other.haystack())
                && Objects.equals(needle(), other.needle())
                && Objects.equals(caseSensitive(), other.caseSensitive());
    }

    @Override
    public int hashCode() {
        return Objects.hash(haystack(), needle(), caseSensitive());
    }


    @Override
    public String getWriteableName() {
        return NAME;
    }
}
