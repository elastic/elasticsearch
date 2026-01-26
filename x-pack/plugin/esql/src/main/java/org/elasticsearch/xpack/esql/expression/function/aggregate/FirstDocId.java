/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.FirstDocIdGroupingAggregatorFunction;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.util.List;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOC_DATA_TYPE;

/**
 * Internal aggregation function that collects the first seen docId per group.
 */
public class FirstDocId extends AggregateFunction implements ToAggregator {

    public FirstDocId(Source source, Expression v) {
        this(source, v, Literal.TRUE, NO_WINDOW);
    }

    public FirstDocId(Source source, Expression field, Expression filter, Expression window) {
        super(source, field, filter, window, emptyList());
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("FirstDocId must be used locally");
    }

    @Override
    protected NodeInfo<FirstDocId> info() {
        return NodeInfo.create(this, FirstDocId::new, field(), filter(), window());
    }

    @Override
    public FirstDocId replaceChildren(List<Expression> newChildren) {
        return new FirstDocId(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }

    @Override
    public FirstDocId withFilter(Expression filter) {
        return new FirstDocId(source(), field(), filter, window());
    }

    @Override
    public DataType dataType() {
        return DataType.DOC_DATA_TYPE;
    }

    @Override
    protected TypeResolution resolveType() {
        return TypeResolutions.isType(field(), dt -> dt == DOC_DATA_TYPE, sourceText(), FIRST, "_doc");
    }

    @Override
    public AggregatorFunctionSupplier supplier() {
        return new FirstDocIdGroupingAggregatorFunction.FunctionSupplier();
    }

}
