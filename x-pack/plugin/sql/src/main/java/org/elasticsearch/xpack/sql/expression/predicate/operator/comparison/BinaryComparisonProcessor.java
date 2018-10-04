/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate.operator.comparison;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.sql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.sql.expression.predicate.operator.comparison.BinaryComparisonProcessor.BinaryComparisonOperation;

import java.io.IOException;
import java.util.function.BiFunction;

public class BinaryComparisonProcessor extends BinaryOperatorProcessor<BinaryComparisonOperation> {
    
    public enum BinaryComparisonOperation implements BiFunction<Object, Object, Boolean> {

        EQ(Comparisons::eq, "=="),
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

        public String symbol() {
            return symbol;
        }

        @Override
        public final Boolean apply(Object left, Object right) {
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
    protected void doWrite(StreamOutput out) throws IOException {
        out.writeEnum(operation());
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}