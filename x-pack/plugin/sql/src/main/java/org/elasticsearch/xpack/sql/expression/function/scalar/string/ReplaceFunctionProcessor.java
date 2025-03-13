/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
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
        if (input == null || pattern == null || replacement == null) {
            return null;
        }
        if ((input instanceof String || input instanceof Character) == false) {
            throw new SqlIllegalArgumentException("A string/char is required; received [{}]", input);
        }
        if ((pattern instanceof String || pattern instanceof Character) == false) {
            throw new SqlIllegalArgumentException("A string/char is required; received [{}]", pattern);
        }
        if ((replacement instanceof String || replacement instanceof Character) == false) {
            throw new SqlIllegalArgumentException("A string/char is required; received [{}]", replacement);
        }

        String inputStr = input instanceof Character ? input.toString() : (String) input;
        String patternStr = pattern instanceof Character ? pattern.toString() : (String) pattern;
        String replacementStr = replacement instanceof Character ? replacement.toString() : (String) replacement;
        checkResultLength(inputStr, patternStr, replacementStr);
        return Strings.replace(inputStr, patternStr, replacementStr);
    }

    private static void checkResultLength(String input, String pattern, String replacement) {
        int patternLen = pattern.length();
        long matches = 0;
        for (int i = input.indexOf(pattern); i >= 0; i = input.indexOf(pattern, i + patternLen)) {
            matches++;
        }
        StringProcessor.checkResultLength(input.length() + matches * (replacement.length() - patternLen));
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
