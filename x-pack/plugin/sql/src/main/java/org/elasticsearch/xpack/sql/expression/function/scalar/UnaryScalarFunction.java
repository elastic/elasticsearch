/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.UnaryPipe;
import org.elasticsearch.xpack.sql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.sql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.tree.Source;

import java.util.List;

import static java.util.Collections.singletonList;

public abstract class UnaryScalarFunction extends ScalarFunction {

    private final Expression field;

    protected UnaryScalarFunction(Source source) {
        super(source);
        this.field = null;
    }

    protected UnaryScalarFunction(Source source, Expression field) {
        super(source, singletonList(field));
        this.field = field;
    }

    @Override
    public final UnaryScalarFunction replaceChildren(List<Expression> newChildren) {
        if (newChildren.size() != 1) {
            throw new IllegalArgumentException("expected [1] child but received [" + newChildren.size() + "]");
        }
        return replaceChild(newChildren.get(0));
    }

    protected abstract UnaryScalarFunction replaceChild(Expression newChild);

    public Expression field() {
        return field;
    }

    @Override
    public final Pipe makePipe() {
        return new UnaryPipe(source(), this, Expressions.pipe(field()), makeProcessor());
    }

    protected abstract Processor makeProcessor();

    @Override
    public boolean foldable() {
        return field.foldable();
    }

    @Override
    public ScriptTemplate asScript() {
        return asScript(field);
    }
}
