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

public class StringContainsFunctionProcessor implements Processor {

    public static final String NAME = "sstc";

    private final Processor string, substring;
    private final boolean isCaseSensitive;

    public StringContainsFunctionProcessor(Processor string, Processor substring, boolean isCaseSensitive) {
        this.string = string;
        this.substring = substring;
        this.isCaseSensitive = isCaseSensitive;
    }

    public StringContainsFunctionProcessor(StreamInput in) throws IOException {
        string = in.readNamedWriteable(Processor.class);
        substring = in.readNamedWriteable(Processor.class);
        isCaseSensitive = in.readBoolean();
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(string);
        out.writeNamedWriteable(substring);
        out.writeBoolean(isCaseSensitive);
    }

    @Override
    public Object process(Object input) {
        return doProcess(string.process(input), substring.process(input), isCaseSensitive);
    }

    public static Object doProcess(Object string, Object substring, boolean isCaseSensitive) {
        if (string == null) {
            return null;
        }

        throwIfNotString(string);
        throwIfNotString(substring);

        String strString = string.toString();
        String strSubstring = substring.toString();

        return StringUtils.stringContains(strString, strSubstring, isCaseSensitive);
    }

    private static void throwIfNotString(Object obj) {
        if (!(obj instanceof String || obj instanceof Character)) {
            throw new EqlIllegalArgumentException("A string/char is required; received [{}]", obj);
        }
    }

    protected Processor string() {
        return string;
    }

    public Processor substring() {
        return substring;
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
        return Objects.equals(string(), other.string())
                && Objects.equals(substring(), other.substring());
    }

    @Override
    public int hashCode() {
        return Objects.hash(string(), substring());
    }


    @Override
    public String getWriteableName() {
        return NAME;
    }
}
