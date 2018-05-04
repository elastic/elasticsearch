/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.container;

import org.elasticsearch.xpack.sql.execution.search.FieldExtraction;
import org.elasticsearch.xpack.sql.execution.search.SqlSourceBuilder;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.ProcessorDefinition;

public class ComputedRef implements FieldExtraction {

    private final ProcessorDefinition processor;

    public ComputedRef(ProcessorDefinition processor) {
        this.processor = processor;
    }

    public ProcessorDefinition processor() {
        return processor;
    }

    @Override
    public boolean supportedByAggsOnlyQuery() {
        return processor.supportedByAggsOnlyQuery();
    }

    @Override
    public void collectFields(SqlSourceBuilder sourceBuilder) {
        processor.collectFields(sourceBuilder);
    }

    @Override
    public String toString() {
        return processor + "(" + processor + ")";
    }
}
