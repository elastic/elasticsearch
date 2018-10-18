/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate.regex;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.gen.processor.BinaryProcessor;
import org.elasticsearch.xpack.sql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.sql.expression.predicate.PredicateBiFunction;

import java.io.IOException;
import java.util.Objects;
import java.util.regex.Pattern;

public class RegexProcessor extends BinaryProcessor {
    
    public static class RegexOperation implements PredicateBiFunction<String, String, Boolean> {

        public static final RegexOperation INSTANCE = new RegexOperation();

        @Override
        public String name() {
            return symbol();
        }

        @Override
        public String symbol() {
            return "REGEX";
        }

        @Override
        public Boolean doApply(String value, String pattern) {
            return match(value, pattern);
        }

        public static Boolean match(Object value, Object pattern) {
            if (value == null || pattern == null) {
                return null;
            }

            Pattern p = Pattern.compile(pattern.toString());
            return p.matcher(value.toString()).matches();
        }
    }

    public static final String NAME = "rgx";

    public RegexProcessor(Processor value, Processor pattern) {
        super(value, pattern);
    }

    public RegexProcessor(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected Boolean doProcess(Object value, Object pattern) {
        return RegexOperation.match(value, pattern);
    }

    @Override
    protected void checkParameter(Object param) {
        if (!(param instanceof String || param instanceof Character)) {
            throw new SqlIllegalArgumentException("A string/char is required; received [{}]", param);
        }
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected void doWrite(StreamOutput out) throws IOException {}

    @Override
    public int hashCode() {
        return Objects.hash(left(), right());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        RegexProcessor other = (RegexProcessor) obj;
        return Objects.equals(left(), other.left()) && Objects.equals(right(), other.right());
    }
}