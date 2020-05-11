/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.string;

import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.function.Function;

public class StringProcessor implements Processor {

    private interface StringFunction<R> {
        default R apply(Object o) {
            if (!(o instanceof String || o instanceof Character)) {
                throw new SqlIllegalArgumentException("A string/char is required; received [{}]", o);
            }

            return doApply(o.toString());
        }

        R doApply(String s);
    }

    private interface NumericFunction<R> {
        default R apply(Object o) {
            if (!(o instanceof Number)) {
                throw new SqlIllegalArgumentException("A number is required; received [{}]", o);
            }

            return doApply((Number) o);
        }

        R doApply(Number s);
    }

    public enum StringOperation {
        ASCII((String s) -> s.length() == 0 ? null : Integer.valueOf(s.charAt(0))),
        CHAR((Number n) -> {
            int i = n.intValue();
            return i < 0 || i > 255 ? null : String.valueOf((char) i);
        }),
        LCASE((String s) -> s.toLowerCase(Locale.ROOT)),
        UCASE((String s) -> s.toUpperCase(Locale.ROOT)),
        LENGTH((String s) -> StringFunctionUtils.trimTrailingWhitespaces(s).length()),
        RTRIM((String s) -> StringFunctionUtils.trimTrailingWhitespaces(s)),
        LTRIM((String s) -> StringFunctionUtils.trimLeadingWhitespaces(s)),
        SPACE((Number n) -> {
            int i = n.intValue();
            if (i < 0) {
                return null;
            }
            char[] spaces = new char[i];
            char whitespace = ' ';
            Arrays.fill(spaces, whitespace);

            return new String(spaces);
        }),
        BIT_LENGTH((String s) -> UnicodeUtil.calcUTF16toUTF8Length(s, 0, s.length()) * 8),
        OCTET_LENGTH((String s) -> UnicodeUtil.calcUTF16toUTF8Length(s, 0, s.length())),
        CHAR_LENGTH(String::length);

        private final Function<Object, Object> apply;

        StringOperation(StringFunction<Object> apply) {
            this.apply = l -> l == null ? null : apply.apply(l);
        }

        StringOperation(NumericFunction<Object> apply) {
            this.apply = l -> l == null ? null : apply.apply(l);
        }

        public final Object apply(Object l) {
            return apply.apply(l);
        }

        /**
         * "translate" the function name ("char") into a function name that is not a reserved keyword in java.
         * Used in {@code InternalSqlScriptUtils#character(Number)}.
         */
        @Override
        public String toString() {
            return this == CHAR ? "character" : super.toString();
        }
    }

    public static final String NAME = "s";

    private final StringOperation processor;

    public StringProcessor(StringOperation processor) {
        this.processor = processor;
    }

    public StringProcessor(StreamInput in) throws IOException {
        processor = in.readEnum(StringOperation.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(processor);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Object process(Object input) {
        return processor.apply(input);
    }

    StringOperation processor() {
        return processor;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        StringProcessor other = (StringProcessor) obj;
        return processor == other.processor;
    }

    @Override
    public int hashCode() {
        return processor.hashCode();
    }

    @Override
    public String toString() {
        return processor.toString();
    }
}