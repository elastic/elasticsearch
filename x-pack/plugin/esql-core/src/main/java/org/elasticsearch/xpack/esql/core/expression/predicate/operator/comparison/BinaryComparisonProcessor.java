/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.esql.core.expression.gen.processor.FunctionalEnumBinaryProcessor;
import org.elasticsearch.xpack.esql.core.expression.gen.processor.Processor;

import java.io.IOException;

public class BinaryComparisonProcessor extends FunctionalEnumBinaryProcessor<Object, Object, Boolean, BinaryComparisonOperation> {

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
