/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.expression.predicate.operator.comparison;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.ql.expression.gen.processor.FunctionalBinaryProcessor;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.ql.expression.predicate.PredicateBiFunction;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparisonProcessor.BinaryComparisonOperation;

import java.io.IOException;
import java.util.function.BiFunction;

public class BinaryComparisonProcessor extends FunctionalBinaryProcessor<Object, Object, Boolean, BinaryComparisonOperation> {

    public enum BinaryComparisonOperation implements PredicateBiFunction<Object, Object, Boolean> {

        EQ(Comparisons::eq, "=="),
        NULLEQ(Comparisons::nulleq, "<=>"),
        NEQ(Comparisons::neq, "!="),
        GT(Comparisons::gt, ">"),
        GTE(Comparisons::gte, ">="),
        LT(Comparisons::lt, "<"),
        LTE(Comparisons::lte, "<=");

        private final BiFunction<Object, Object, Boolean> process;
        private final String symbol;

        BinaryComparisonOperation(BiFunction<Object, Object, Boolean> process, String symbol) {
            this.process = process;
            this.symbol = symbol;
        }

        @Override
        public String symbol() {
            return symbol;
        }

        @Override
        public Boolean apply(Object left, Object right) {
            if (this != NULLEQ && (left == null || right == null)) {
                return null;
            }
            return doApply(left, right);
        }

        @Override
        public final Boolean doApply(Object left, Object right) {
            return process.apply(left, right);
        }

        @Override
        public String toString() {
            return symbol;
        }
    }

    public static final String NAME = "cb";

    public BinaryComparisonProcessor(Processor left, Processor right, BinaryComparisonOperation operation) {
        super(left, right, operation);
    }

    public BinaryComparisonProcessor(StreamInput in) throws IOException {
        super(in, i -> i.readEnum(BinaryComparisonOperation.class));
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Object process(Object input) {
        if (function() == BinaryComparisonOperation.NULLEQ) {
            return doProcess(left().process(input), right().process(input));
        }
        return super.process(input);
    }
}
