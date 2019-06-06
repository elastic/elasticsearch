/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.gen.pipeline;

import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.execution.search.SqlSourceBuilder;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.sql.tree.Source;

/**
 * Implementation common to most subclasses of
 * {@link NonExecutableInput} but not shared by all.
 */
abstract class CommonNonExecutableInput<T> extends NonExecutableInput<T> {
    CommonNonExecutableInput(Source source, Expression expression, T context) {
        super(source, expression, context);
    }

    @Override
    public final Processor asProcessor() {
        throw new SqlIllegalArgumentException("Unresolved input - needs resolving first");
    }

    @Override
    public final Pipe resolveAttributes(AttributeResolver resolver) {
        return this;
    }

    @Override
    public final void collectFields(SqlSourceBuilder sourceBuilder) {
        // Nothing to extract
    }
}
