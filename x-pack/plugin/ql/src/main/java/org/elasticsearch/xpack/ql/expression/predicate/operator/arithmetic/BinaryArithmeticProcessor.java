/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.ql.expression.gen.processor.FunctionalBinaryProcessor;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;

import java.io.IOException;

public final class BinaryArithmeticProcessor extends FunctionalBinaryProcessor<Object, Object, Object, BinaryArithmeticOperation> {
    
    public static final String NAME = "abn";

    public BinaryArithmeticProcessor(Processor left, Processor right, BinaryArithmeticOperation operation) {
        super(left, right, operation);
    }

    public BinaryArithmeticProcessor(StreamInput in) throws IOException {
        super(in, i -> i.readNamedWriteable(BinaryArithmeticOperation.class));
    }

    @Override
    protected void doWrite(StreamOutput out) throws IOException {
        out.writeNamedWriteable(function());
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Object doProcess(Object left, Object right) {
        BinaryArithmeticOperation f = function();

        if (left == null || right == null) {
            return null;
        }

        return f.apply(left, right);
    }
}