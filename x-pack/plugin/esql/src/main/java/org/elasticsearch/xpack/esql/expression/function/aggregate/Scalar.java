/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;

/**
 * Returns the sample value if there is exactly one element, otherwise returns NaN.
 */
public class Scalar extends AggregateFunction implements SurrogateExpression {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Scalar", Scalar::new);

    public Scalar(Source source, Expression field) {
        this(source, field, Literal.TRUE, NO_WINDOW);
    }

    public Scalar(Source source, Expression field, Expression filter, Expression window) {
        super(source, field, filter, window, emptyList());
    }

    private Scalar(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<Scalar> info() {
        return NodeInfo.create(this, Scalar::new, field(), filter(), window());
    }

    @Override
    public AggregateFunction withFilter(Expression filter) {
        return new Scalar(source(), field(), filter, window());
    }

    @Override
    public Scalar replaceChildren(List<Expression> newChildren) {
        return new Scalar(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }

    @Override
    public DataType dataType() {
        // Scalar can return Nan so its output is always double.
        return DOUBLE;
    }

    @Override
    protected TypeResolution resolveType() {
        return isType(field(), dt -> dt.isNumeric() || DataType.isCounter(dt), sourceText(), DEFAULT, "numeric");
    }

    @Override
    public Expression surrogate() {
        var s = source();
        var field = field();

        Count count = new Count(s, field, filter(), window());
        Max max = new Max(s, field, filter(), window());
        Literal one = new Literal(s, 1L, LONG);
        Literal nan = new Literal(s, Double.NaN, DOUBLE);

        return new Case(s, new Equals(s, count, one), List.of(new ToDouble(s, max), nan));
    }
}
