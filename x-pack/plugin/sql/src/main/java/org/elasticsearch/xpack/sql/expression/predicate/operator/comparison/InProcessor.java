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

    private final Processor left;
    private final List<Processor> rightList;

    InProcessor(Processor left, List<Processor> rightList) {
        this.left = left;
        this.rightList = rightList;
    }

    InProcessor(StreamInput in) throws IOException {
        left = in.readNamedWriteable(Processor.class);
        rightList = in.readNamedWriteableList(Processor.class);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(left);
        out.writeNamedWriteableList(rightList);
    }

    @Override
    public Object process(Object input) {
        Object leftValue = left.process(input);
        for (Processor p : rightList) {
            Boolean compResult = Comparisons.eq(leftValue, p.process(input));
            if (compResult != null && compResult) {
                return true;
            }
        }
        return false;
    }
}
