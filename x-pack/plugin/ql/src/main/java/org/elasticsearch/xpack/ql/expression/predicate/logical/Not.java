/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.expression.predicate.logical;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.ql.expression.gen.script.Scripts;
import org.elasticsearch.xpack.ql.expression.predicate.Negatable;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isBoolean;

public class Not extends UnaryScalarFunction implements Negatable<Expression> {

    public Not(Source source, Expression child) {
        super(source, child);
    }

    @Override
    protected NodeInfo<Not> info() {
        return NodeInfo.create(this, Not::new, field());
    }

    @Override
    protected Not replaceChild(Expression newChild) {
        return new Not(source(), newChild);
    }

    @Override
    protected TypeResolution resolveType() {
        if (DataTypes.BOOLEAN == field().dataType()) {
            return TypeResolution.TYPE_RESOLVED;
        }
        return isBoolean(field(), sourceText(), DEFAULT);
    }

    @Override
    public Object fold() {
        return NotProcessor.INSTANCE.process(field().fold());
    }

    @Override
    protected Processor makeProcessor() {
        return NotProcessor.INSTANCE;
    }

    @Override
    public String processScript(String script) {
        return Scripts.formatTemplate(Scripts.QL_SCRIPTS + ".not(" + script + ")");
    }

    @Override
    protected Expression canonicalize() {
        if (field() instanceof Negatable) {
            return ((Negatable) field()).negate().canonical();
        }
        return super.canonicalize();
    }

    @Override
    public Expression negate() {
        return field();
    }

    @Override
    public DataType dataType() {
        return DataTypes.BOOLEAN;
    }

    static Expression negate(Expression exp) {
        return exp instanceof Negatable ? ((Negatable) exp).negate() : new Not(exp.source(), exp);
    }
}
