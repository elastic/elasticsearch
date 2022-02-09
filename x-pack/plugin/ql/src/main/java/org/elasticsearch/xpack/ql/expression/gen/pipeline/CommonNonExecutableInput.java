/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.expression.gen.pipeline;

import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.execution.search.QlSourceBuilder;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.ql.tree.Source;

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
        throw new QlIllegalArgumentException("Unresolved input - needs resolving first");
    }

    @Override
    public final Pipe resolveAttributes(AttributeResolver resolver) {
        return this;
    }

    @Override
    public final void collectFields(QlSourceBuilder sourceBuilder) {
        // Nothing to extract
    }
}
