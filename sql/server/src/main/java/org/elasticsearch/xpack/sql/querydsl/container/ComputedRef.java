/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.container;

import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.ProcessorDefinition;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.ReferenceInput;

import java.util.concurrent.atomic.AtomicInteger;

public class ComputedRef implements ColumnReference {

    private final ProcessorDefinition processor;
    private final int depth;

    public ComputedRef(ProcessorDefinition processor) {
        this.processor = processor;

        // compute maximum depth
        AtomicInteger d = new AtomicInteger(0);
        processor.forEachDown(i -> {
            ColumnReference ref = i.context();
            if (ref.depth() > d.get()) {
                d.set(ref.depth());
            }
        }, ReferenceInput.class);

        depth = d.get();
    }

    public ProcessorDefinition processor() {
        return processor;
    }

    @Override
    public int depth() {
        return depth;
    }

    @Override
    public String toString() {
        return processor + "(" + processor + ")";
    }
}
