/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.expression.function.scalar;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.UnaryPipe;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.tree.Source;

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
    public Object fold() {
        return makeProcessor().process(field().fold());
    }

    @Override
    public ScriptTemplate asScript() {
        return asScript(field);
    }
}
