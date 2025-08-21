/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.util.List;

import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_PERIOD;

public class ToDatePeriod extends FoldablesConvertFunction {

    @FunctionInfo(
        returnType = "date_period",
        description = "Converts an input value into a `date_period` value.",
        examples = @Example(file = "convert", tag = "castToDatePeriod")
    )
    public ToDatePeriod(
        Source source,
        @Param(
            name = "field",
            type = { "date_period", "keyword", "text" },
            description = "Input value. The input is a valid constant date period expression."
        ) Expression v
    ) {
        super(source, v);
    }

    @Override
    public DataType dataType() {
        return DATE_PERIOD;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ToDatePeriod(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ToDatePeriod::new, field());
    }
}
