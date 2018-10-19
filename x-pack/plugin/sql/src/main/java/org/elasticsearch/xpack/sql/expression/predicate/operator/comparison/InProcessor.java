/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate.operator.comparison;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.sql.expression.gen.processor.Processor;

import java.io.IOException;
import java.util.List;

class InProcessor implements Processor {

    public static final String NAME = "in";

    private final List<Processor> processsors;

    InProcessor(List<Processor> processors) {
        this.processsors = processors;
    }

    InProcessor(StreamInput in) throws IOException {
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
        Object leftValue = processsors.get(0).process(input);
        for (int i = 1; i < processsors.size(); i++) {
            Boolean compResult = Comparisons.eq(leftValue, processsors.get(i).process(input));
            if (compResult != null && compResult) {
                return true;
            }
        }
        return false;
    }
}
