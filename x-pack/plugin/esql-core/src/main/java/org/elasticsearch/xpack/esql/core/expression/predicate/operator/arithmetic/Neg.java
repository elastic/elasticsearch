/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression.predicate.operator.arithmetic;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNumeric;

/**
 * Negation function (@{code -x}).
 */
public class Neg extends UnaryScalarFunction {

    public Neg(Source source, Expression field) {
        super(source, field);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected NodeInfo<Neg> info() {
        return NodeInfo.create(this, Neg::new, field());
    }

    @Override
    protected Neg replaceChild(Expression newChild) {
        return new Neg(source(), newChild);
    }

    @Override
    protected TypeResolution resolveType() {
        return isNumeric(field(), sourceText(), DEFAULT);
    }

    @Override
    public Object fold(FoldContext ctx) {
        return Arithmetics.negate((Number) field().fold(ctx));
    }

    @Override
    public DataType dataType() {
        return field().dataType();
    }
}
