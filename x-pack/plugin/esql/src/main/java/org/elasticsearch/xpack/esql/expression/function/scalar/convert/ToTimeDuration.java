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

import static org.elasticsearch.xpack.esql.core.type.DataType.TIME_DURATION;

public class ToTimeDuration extends FoldablesConvertFunction {

    @FunctionInfo(
        returnType = "time_duration",
        description = "Converts an input value into a `time_duration` value.",
        examples = @Example(file = "convert", tag = "castToTimeDuration")
    )
    public ToTimeDuration(
        Source source,
        @Param(
            name = "field",
            type = { "time_duration", "keyword", "text" },
            description = "Input value. The input is a valid constant time duration expression."
        ) Expression v
    ) {
        super(source, v);
    }

    @Override
    public DataType dataType() {
        return TIME_DURATION;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ToTimeDuration(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ToTimeDuration::new, field());
    }
}
