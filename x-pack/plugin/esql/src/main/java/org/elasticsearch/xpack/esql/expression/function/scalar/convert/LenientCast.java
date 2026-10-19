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
 * The lenient cast operator {@code field?::type}, wrapping the conversion that {@code field::type} would build.
 */
public final class LenientCast extends UnaryExpression {

    public LenientCast(Source source, Expression convert) {
        super(source, convert);
    }

    /**
     * The wrapped conversion, e.g. {@code ToLong(field)} for {@code field?::long}.
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
