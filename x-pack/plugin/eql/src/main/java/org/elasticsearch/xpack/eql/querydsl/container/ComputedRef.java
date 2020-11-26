/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.querydsl.container;

import org.elasticsearch.xpack.ql.execution.search.FieldExtraction;
import org.elasticsearch.xpack.ql.execution.search.QlSourceBuilder;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;

public class ComputedRef implements FieldExtraction {

    private final Pipe processor;

    public ComputedRef(Pipe processor) {
        this.processor = processor;
    }

    public Pipe processor() {
        return processor;
    }

    @Override
    public boolean supportedByAggsOnlyQuery() {
        return processor.supportedByAggsOnlyQuery();
    }

    @Override
    public void collectFields(QlSourceBuilder sourceBuilder) {
        processor.collectFields(sourceBuilder);
    }

    @Override
    public String toString() {
        return processor + "(" + processor + ")";
    }
}

