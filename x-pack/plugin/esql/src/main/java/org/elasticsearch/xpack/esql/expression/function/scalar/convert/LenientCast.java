/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.UnaryExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * The lenient cast operator {@code field::~type}, wrapping the conversion that {@code field::type} would build.
 * <p>
 * On a union-typed field, the indices whose type cannot convert load as null, with a warning, instead of failing the query.
 * Everywhere else it behaves exactly like the wrapped conversion.
 * <p>
 * It only exists during analysis: {@code ResolveUnionTypes} replaces it with the resolved union field or with the wrapped
 * conversion, and the verifier rejects any that remain. That keeps it out of the optimizer, serialization and execution, so
 * code there only ever sees plain conversions.
 */
public final class LenientCast extends UnaryExpression {

    public LenientCast(Source source, Expression convert) {
        super(source, convert);
    }

    /**
     * The wrapped conversion, e.g. {@code ToLong(field)} for {@code field::~long}.
     */
    public ConvertFunction convert() {
        return (ConvertFunction) child();
    }

    @Override
    protected LenientCast replaceChild(Expression newChild) {
        return new LenientCast(source(), newChild);
    }

    @Override
    protected NodeInfo<LenientCast> info() {
        return NodeInfo.create(this, LenientCast::new, child());
    }

    @Override
    public void writeTo(StreamOutput out) {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }
}
