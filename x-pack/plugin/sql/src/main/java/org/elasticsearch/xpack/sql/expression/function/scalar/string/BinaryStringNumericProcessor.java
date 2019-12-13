/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.string;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.ql.expression.gen.processor.FunctionalBinaryProcessor;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.BinaryStringNumericProcessor.BinaryStringNumericOperation;

import java.io.IOException;
import java.util.function.BiFunction;

/**
 * Processor class covering string manipulating functions that have the first parameter as string,
 * second parameter as numeric and a string result.
 */
public class BinaryStringNumericProcessor extends FunctionalBinaryProcessor<String, Number, String, BinaryStringNumericOperation> {
    
    public enum BinaryStringNumericOperation implements BiFunction<String, Number, String> {
        LEFT((s,c) -> {
            int i = c.intValue();
            if (i < 0) {
                return "";
            }
            return i > s.length() ? s : s.substring(0, i);
        }),
        RIGHT((s,c) -> {
            int i = c.intValue();
            if (i < 0) {
                return "";
            }
            return i > s.length() ? s : s.substring(s.length() - i);
        }),
        REPEAT((s,c) -> {
            int i = c.intValue();
            if (i <= 0) {
                return null;
            }
            
            StringBuilder sb = new StringBuilder(s.length() * i);
            for (int j = 0; j < i; j++) {
                sb.append(s);
            }
            return sb.toString();
        });

        BinaryStringNumericOperation(BiFunction<String, Number, String> op) {
            this.op = op;
        }
        
        private final BiFunction<String, Number, String> op;

        @Override
        public String apply(String stringExp, Number count) {
            if (stringExp == null || count == null) {
                return null;
            }
            return op.apply(stringExp, count);
        }
    }

    public static final String NAME = "sn";

    public BinaryStringNumericProcessor(Processor left, Processor right, BinaryStringNumericOperation operation) {
        super(left, right, operation);
    }

    public BinaryStringNumericProcessor(StreamInput in) throws IOException {
        super(in, i -> i.readEnum(BinaryStringNumericOperation.class));
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Object doProcess(Object left, Object right) {
        if (!(left instanceof String || left instanceof Character)) {
            throw new SqlIllegalArgumentException("A string/char is required; received [{}]", left);
        }
        if (!(right instanceof Number)) {
            throw new SqlIllegalArgumentException("A number is required; received [{}]", right);
        }

        return super.doProcess(left.toString(), right);
    }
}