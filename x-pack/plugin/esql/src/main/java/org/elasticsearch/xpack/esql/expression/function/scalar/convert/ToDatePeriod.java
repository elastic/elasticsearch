/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_PERIOD;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;

public class ToDatePeriod extends AbstractConvertFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "ToDatePeriod",
        ToDatePeriod::new
    );

    private static final Map<DataType, BuildFactory> EVALUATORS = Map.ofEntries(
        Map.entry(DATE_PERIOD, (fieldEval, source) -> fieldEval),
        Map.entry(KEYWORD, (fieldEval, source) -> fieldEval),
        Map.entry(TEXT, (fieldEval, source) -> fieldEval)
    );

    @FunctionInfo(
        returnType = "date_period",
        description = "Converts an input value into a date_period value.",
        examples = @Example(description = "Converts an input value into a date_period value.", file = "convert", tag = "castToDatePeriod")
    )
    public ToDatePeriod(
        Source source,
        @Param(name = "field", type = { "date_period", "keyword", "text" }, description = "Input value.") Expression v
    ) {
        super(source, v);
    }

    private ToDatePeriod(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected Map<DataType, BuildFactory> factories() {
        return EVALUATORS;
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

    @Override
    public final Object fold() {
        if (field instanceof Literal l) {
            Object v = l.value();
            if (v instanceof BytesRef b) {
                return EsqlDataTypeConverter.parseTemporalAmount(b.utf8ToString(), DATE_PERIOD);
            }
        }
        return null;
    }
}
