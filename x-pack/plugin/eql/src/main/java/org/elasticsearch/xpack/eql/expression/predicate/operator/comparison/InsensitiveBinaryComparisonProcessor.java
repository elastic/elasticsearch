/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql.expression.predicate.operator.comparison;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.ql.expression.gen.processor.FunctionalEnumBinaryProcessor;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.ql.expression.predicate.PredicateBiFunction;

import java.io.IOException;
import java.util.function.BiFunction;

public class InsensitiveBinaryComparisonProcessor extends FunctionalEnumBinaryProcessor<
    Object,
    Object,
    Boolean,
    InsensitiveBinaryComparisonProcessor.InsensitiveBinaryComparisonOperation> {

    public enum InsensitiveBinaryComparisonOperation implements PredicateBiFunction<Object, Object, Boolean> {

        SEQ(StringComparisons::insensitiveEquals, ":"),
        SNEQ(StringComparisons::insensitiveNotEquals, "!:");

        private final BiFunction<Object, Object, Boolean> process;
        private final String symbol;

        InsensitiveBinaryComparisonOperation(BiFunction<Object, Object, Boolean> process, String symbol) {
            this.process = process;
            this.symbol = symbol;
        }

        @Override
        public String symbol() {
            return symbol;
        }

        @Override
        public Boolean apply(Object left, Object right) {
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

    public static final String NAME = "cscb";

    public InsensitiveBinaryComparisonProcessor(Processor left, Processor right, InsensitiveBinaryComparisonOperation operation) {
        super(left, right, operation);
    }

    public InsensitiveBinaryComparisonProcessor(StreamInput in) throws IOException {
        super(in, i -> i.readEnum(InsensitiveBinaryComparisonOperation.class));
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Object process(Object input) {
        return doProcess(left().process(input), right().process(input));
    }
}
