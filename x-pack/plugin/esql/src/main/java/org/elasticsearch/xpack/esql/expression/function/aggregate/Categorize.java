/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.CategorizeBytesRefAggregatorFunctionSupplier;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;

public class Categorize extends AggregateFunction implements ToAggregator {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "Categorize",
        Categorize::new
    );

    @FunctionInfo(returnType = { "keyword", "text" }, preview = true, description = "... ", appendix = """
        [WARNING]
        ====
        This can use a significant amount of memory and ES|QL doesn't yet
        grow aggregations beyond memory. So this aggregation will work until
        it is used to collect more values than can fit into memory. Once it
        collects too many values it will fail the query with
        a <<circuit-breaker-errors, Circuit Breaker Error>>.
        ====""", isAggregation = true, examples = @Example(file = "string", tag = "values-grouped"))
    public Categorize(Source source, @Param(name = "field", type = { "keyword", "text" }) Expression v) {
        super(source, v);
    }

    private Categorize(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<Categorize> info() {
        return NodeInfo.create(this, Categorize::new, field());
    }

    @Override
    public Categorize replaceChildren(List<Expression> newChildren) {
        return new Categorize(source(), newChildren.get(0));
    }

    @Override
    public DataType dataType() {
        return field().dataType();
    }

    @Override
    protected TypeResolution resolveType() {
        return isString(field(), sourceText(), DEFAULT);
    }

    @Override
    public AggregatorFunctionSupplier supplier(List<Integer> inputChannels) {
        return new CategorizeBytesRefAggregatorFunctionSupplier(inputChannels);
    }
}
