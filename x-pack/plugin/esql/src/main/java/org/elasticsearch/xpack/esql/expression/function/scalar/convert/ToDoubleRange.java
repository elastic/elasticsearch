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
import org.elasticsearch.compute.ann.ConvertEvaluator;
import org.elasticsearch.compute.data.DoubleRangeBlockBuilder;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE_RANGE;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;

public class ToDoubleRange extends AbstractConvertFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "ToDoubleRange",
        ToDoubleRange::new
    );
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(ToDoubleRange.class)
        .unary(ToDoubleRange::new)
        .name("to_double_range", "to_doublerange");

    private static final Map<DataType, BuildFactory> EVALUATORS = Map.ofEntries(
        Map.entry(DOUBLE_RANGE, (source, fieldEval) -> fieldEval),
        Map.entry(KEYWORD, ToDoubleRangeFromStringEvaluator.Factory::new),
        Map.entry(TEXT, ToDoubleRangeFromStringEvaluator.Factory::new)
    );

    @FunctionInfo(
        returnType = "double_range",
        preview = true,
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.6.0") },
        briefSummary = "Converts a value to a double_range.",
        description = """
            Converts an input value to a `double_range` value.
            A string will be parsed as a double range in the format `start..end`, where start and end are \
            double-precision floating-point numbers. The range is half-open `[start, end)`.""",
        examples = { @Example(file = "double_range", tag = "to_double_range-str") }
    )
    public ToDoubleRange(
        Source source,
        @Param(
            name = "field",
            type = { "double_range", "keyword", "text" },
            description = "Input value. The input can be a single- or multi-valued column or an expression."
        ) Expression field
    ) {
        super(source, field);
    }

    private ToDoubleRange(StreamInput in) throws IOException {
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
        return DOUBLE_RANGE;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ToDoubleRange(source(), newChildren.getFirst());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ToDoubleRange::new, field());
    }

    @ConvertEvaluator(extraName = "FromString", warnExceptions = { IllegalArgumentException.class })
    static DoubleRangeBlockBuilder.DoubleRange fromKeyword(BytesRef in) {
        return EsqlDataTypeConverter.parseDoubleRange(in.utf8ToString());
    }
}
