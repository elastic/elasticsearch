/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.expression.gen.pipeline;

import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.ql.tree.Source;

public abstract class NonExecutableInput<T> extends LeafInput<T> {
    NonExecutableInput(Source source, Expression expression, T context) {
        super(source, expression, context);
    }

    @Override
    public boolean resolved() {
        return false;
    }

    @Override
    public Processor asProcessor() {
        throw new QlIllegalArgumentException("Unresolved input - needs resolving first");
    }
}
