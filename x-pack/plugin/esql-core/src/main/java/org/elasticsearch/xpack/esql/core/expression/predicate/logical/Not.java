/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression.predicate.logical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.esql.core.expression.predicate.Negatable;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isBoolean;

public class Not extends UnaryScalarFunction implements Negatable<Expression> {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Not", Not::new);

    public Not(Source source, Expression child) {
        super(source, child);
    }

    private Not(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
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
        if (DataType.BOOLEAN == field().dataType()) {
            return TypeResolution.TYPE_RESOLVED;
        }
        return isBoolean(field(), sourceText(), DEFAULT);
    }

    @Override
    public Object fold() {
        return apply(field().fold());
    }

    private static Boolean apply(Object input) {
        if (input == null) {
            return null;
        }

        if ((input instanceof Boolean) == false) {
            throw new QlIllegalArgumentException("A boolean is required; received {}", input);
        }

        return ((Boolean) input).booleanValue() ? Boolean.FALSE : Boolean.TRUE;
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
        return DataType.BOOLEAN;
    }

    static Expression negate(Expression exp) {
        return exp instanceof Negatable ? ((Negatable) exp).negate() : new Not(exp.source(), exp);
    }
}
