/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate.operator.comparison;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.Comparisons;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class InProcessor implements Processor {

    public static final String NAME = "in";

    private final List<Processor> processsors;

    InProcessor(List<Processor> processors) {
        this.processsors = processors;
    }

    public InProcessor(StreamInput in) throws IOException {
        processsors = in.readNamedWriteableList(Processor.class);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteableList(processsors);
    }

    @Override
    public Object process(Object input) {
        Object leftValue = processsors.get(processsors.size() - 1).process(input);
        return apply(leftValue, process(processsors.subList(0, processsors.size() - 1), leftValue));
    }

    private static List<Object> process(List<Processor> processors, Object input) {
        List<Object> values = new ArrayList<>(processors.size());
        for (Processor p : processors) {
            values.add(p.process(input));
        }
        return values;
    }

    public static Boolean apply(Object input, List<Object> values) {
        Boolean result = Boolean.FALSE;
        for (Object v : values) {
            Boolean compResult = Comparisons.eq(input, v);
            if (compResult == null) {
                result = null;
            } else if (compResult == Boolean.TRUE) {
                return Boolean.TRUE;
            }
        }
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        InProcessor that = (InProcessor) o;
        return Objects.equals(processsors, that.processsors);
    }

    @Override
    public int hashCode() {
        return Objects.hash(processsors);
    }
}
