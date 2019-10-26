/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate.conditional;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.sql.expression.gen.processor.Processor;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xpack.sql.expression.predicate.operator.comparison.BinaryComparisonProcessor.BinaryComparisonOperation.EQ;

public class NullIfProcessor implements Processor {

    public static final String NAME = "nni";

    private final Processor leftProcessor;
    private final Processor rightProcessor;


    public NullIfProcessor(Processor leftProcessor, Processor rightProcessor) {
        this.leftProcessor = leftProcessor;
        this.rightProcessor = rightProcessor;
    }

    public NullIfProcessor(StreamInput in) throws IOException {
        leftProcessor = in.readNamedWriteable(Processor.class);
        rightProcessor = in.readNamedWriteable(Processor.class);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(leftProcessor);
        out.writeNamedWriteable(rightProcessor);
    }

    @Override
    public Object process(Object input) {
        Object leftValue = leftProcessor.process(input);
        Object rightValue = rightProcessor.process(input);
        return apply(leftValue, rightValue);
    }

    public static Object apply(Object leftValue, Object rightValue) {
        if (EQ.apply(leftValue, rightValue) == Boolean.TRUE) {
            return null;
        }
        return leftValue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NullIfProcessor that = (NullIfProcessor) o;
        return Objects.equals(leftProcessor, that.leftProcessor) &&
            Objects.equals(rightProcessor, that.rightProcessor);
    }

    @Override
    public int hashCode() {
        return Objects.hash(leftProcessor, rightProcessor);
    }
}
