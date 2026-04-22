/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;

/**
 * Converts a value to an expression of type {@code TEXT}. This is different from the {@link ToString} function, which converts to
 * {@code KEYWORD}. {@code TEXT} and {@code KEYWORD} data types are treated in ES|QL almost the same, the main difference is that
 * {@code TEXT} is considered to be analyzed, while {@code KEYWORD} is not.
 * This matters for functions like {@link org.elasticsearch.xpack.esql.expression.function.fulltext.Match} which will treat these data types
 * differently.
 */
public class ToText extends AbstractConvertFunction implements EvaluatorMapper {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "ToText", ToText::new);
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(ToText.class).unary(ToText::new).name("to_text");

    private static final Map<DataType, BuildFactory> EVALUATORS = Map.ofEntries(
        Map.entry(KEYWORD, (source, fieldEval) -> fieldEval),
        Map.entry(TEXT, (source, fieldEval) -> fieldEval)
    );

    @FunctionInfo(
        returnType = "text",
        description = "Converts an input value into a text.",
        examples = { @Example(file = "convert", tag = "to_text") }
    )
    public ToText(
        Source source,
        @Param(
            name = "field",
            type = { "keyword", "text" },
            description = "Input value. The input can be a single- or multi-valued column or an expression."
        ) Expression v
    ) {
        super(source, v);
    }

    private ToText(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected Map<DataType, BuildFactory> factories() {
        return new HashMap<>(EVALUATORS);
    }

    @Override
    public DataType dataType() {
        return TEXT;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ToText(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ToText::new, field());
    }
}
