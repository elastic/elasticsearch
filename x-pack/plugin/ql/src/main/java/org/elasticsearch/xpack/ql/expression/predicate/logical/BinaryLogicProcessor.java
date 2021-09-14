/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.expression.predicate.logical;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.gen.processor.FunctionalEnumBinaryProcessor;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.ql.expression.predicate.PredicateBiFunction;
import org.elasticsearch.xpack.ql.expression.predicate.logical.BinaryLogicProcessor.BinaryLogicOperation;

import java.io.IOException;
import java.util.function.BiFunction;

public class BinaryLogicProcessor extends FunctionalEnumBinaryProcessor<Boolean, Boolean, Boolean, BinaryLogicOperation> {

    public enum BinaryLogicOperation implements PredicateBiFunction<Boolean, Boolean, Boolean> {

        AND((l, r) -> {
            if (Boolean.FALSE.equals(l) || Boolean.FALSE.equals(r)) {
                return Boolean.FALSE;
            }
            if (l == null || r == null) {
                return null;
            }
            return Boolean.logicalAnd(l.booleanValue(), r.booleanValue());
        }, "AND"),
        OR((l, r) -> {
            if (Boolean.TRUE.equals(l) || Boolean.TRUE.equals(r)) {
                return Boolean.TRUE;
            }
            if (l == null || r == null) {
                return null;
            }
            return Boolean.logicalOr(l.booleanValue(), r.booleanValue());
        }, "OR");

        private final BiFunction<Boolean, Boolean, Boolean> process;
        private final String symbol;

        BinaryLogicOperation(BiFunction<Boolean, Boolean, Boolean> process, String symbol) {
            this.process = process;
            this.symbol = symbol;
        }

        @Override
        public String symbol() {
            return symbol;
        }

        @Override
        public Boolean apply(Boolean left, Boolean right) {
            return process.apply(left, right);
        }

        @Override
        public final Boolean doApply(Boolean left, Boolean right) {
            return null;
        }

        @Override
        public String toString() {
            return symbol;
        }
    }

    public static final String NAME = "lb";

    public BinaryLogicProcessor(Processor left, Processor right, BinaryLogicOperation operation) {
        super(left, right, operation);
    }

    public BinaryLogicProcessor(StreamInput in) throws IOException {
        super(in, i -> i.readEnum(BinaryLogicOperation.class));
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected void checkParameter(Object param) {
        if (param != null && (param instanceof Boolean) == false) {
            throw new QlIllegalArgumentException("A boolean is required; received {}", param);
        }
    }

    @Override
    public Object process(Object input) {
        Object l = left().process(input);
        checkParameter(l);
        Object r = right().process(input);
        checkParameter(r);

        return doProcess(l, r);
    }
}
