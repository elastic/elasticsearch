/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;

/**
 * Basic wrapper for expressions declared with a nested filter (typically in stats).
 * Used during parsing to attach the filter to the nested expression - it is expected the two
 * get fused later on.
 */
// TODO: This class should implement SurrogateExpression but it doesn't due to its use on folding aggregates
// see https://github.com/elastic/elasticsearch/issues/100634#issuecomment-2400665066
public class FilteredExpression extends Expression {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "FilteredExpression",
        FilteredExpression::new
    );

    private final Expression delegate;
    private final Expression filter;

    public FilteredExpression(Source source, Expression delegate, Expression filter) {
        super(source, asList(delegate, filter));
        this.delegate = delegate;
        this.filter = filter;
    }

    public FilteredExpression(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
    }

    public Expression surrogate() {
        return delegate.transformUp(AggregateFunction.class, af -> af.withFilter(filter));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(delegate);
        out.writeNamedWriteable(filter);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public Expression delegate() {
        return delegate;
    }

    public Expression filter() {
        return filter;
    }

    @Override
    public DataType dataType() {
        return delegate.dataType();
    }

    @Override
    public Nullability nullable() {
        return delegate.nullable();
    }

    @Override
    protected NodeInfo<FilteredExpression> info() {
        return NodeInfo.create(this, FilteredExpression::new, delegate, filter);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new FilteredExpression(source(), newChildren.get(0), newChildren.get(1));
    }
}
