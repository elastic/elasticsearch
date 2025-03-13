/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.string;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.ql.expression.gen.processor.BinaryProcessor;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.ql.util.StringUtils;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xpack.sql.expression.function.scalar.string.StringProcessor.checkResultLength;

public class ConcatFunctionProcessor extends BinaryProcessor {

    public static final String NAME = "scon";

    public ConcatFunctionProcessor(Processor source1, Processor source2) {
        super(source1, source2);
    }

    public ConcatFunctionProcessor(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public Object process(Object input) {
        Object l = left().process(input);
        checkParameter(l);
        Object r = right().process(input);
        checkParameter(r);
        return doProcess(l, r);
    }

    @Override
    protected Object doProcess(Object left, Object right) {
        return process(left, right);
    }

    /**
     * Used in Painless scripting
     */
    public static Object process(Object source1, Object source2) {
        if (source1 == null && source2 == null) {
            return StringUtils.EMPTY;
        }
        if (source1 == null) {
            return source2;
        }
        if (source2 == null) {
            return source1;
        }
        if ((source1 instanceof String || source1 instanceof Character) == false) {
            throw new SqlIllegalArgumentException("A string/char is required; received [{}]", source1);
        }
        if ((source2 instanceof String || source2 instanceof Character) == false) {
            throw new SqlIllegalArgumentException("A string/char is required; received [{}]", source2);
        }

        String str1 = source1.toString();
        String str2 = source2.toString();
        checkResultLength(str1.length() + str2.length());
        return str1.concat(str2);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ConcatFunctionProcessor other = (ConcatFunctionProcessor) obj;
        return Objects.equals(left(), other.left()) && Objects.equals(right(), other.right());
    }

    @Override
    public int hashCode() {
        return Objects.hash(left(), right());
    }

    @Override
    protected void doWrite(StreamOutput out) throws IOException {}
}
