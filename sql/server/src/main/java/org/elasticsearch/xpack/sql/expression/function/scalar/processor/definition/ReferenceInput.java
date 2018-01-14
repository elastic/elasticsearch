/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition;

import org.elasticsearch.xpack.sql.execution.search.FieldExtraction;
import org.elasticsearch.xpack.sql.execution.search.SqlSourceBuilder;
import org.elasticsearch.xpack.sql.expression.Expression;

public class ReferenceInput extends NonExecutableInput<FieldExtraction> {
    public ReferenceInput(Expression expression, FieldExtraction context) {
        super(expression, context);
    }

    @Override
    public final boolean supportedByAggsOnlyQuery() {
        return false;
    }

    @Override
    public ProcessorDefinition resolveAttributes(AttributeResolver resolver) {
        return this;
    }

    @Override
    public final void collectFields(SqlSourceBuilder sourceBuilder) {
        context().collectFields(sourceBuilder);
    }

    @Override
    public int depth() {
        return context().depth();
    }
}
