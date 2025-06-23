/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.UnaryScalarFunction;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;

public class ByteLength extends UnaryScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "ByteLength",
        ByteLength::new
    );

    @FunctionInfo(
        returnType = "integer",
        description = "Returns the byte length of a string.",
        note = "All strings are in UTF-8, so a single character can use multiple bytes.",
        examples = @Example(file = "eval", tag = "byteLength")
    )
    public ByteLength(
        Source source,
        @Param(
            name = "string",
            type = { "keyword", "text" },
            description = "String expression. If `null`, the function returns `null`."
        ) Expression field
    ) {
        super(source, field);
    }

    private ByteLength(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return DataType.INTEGER;
    }

    @Override
    protected TypeResolution resolveType() {
        return childrenResolved() ? isString(field(), sourceText(), DEFAULT) : new TypeResolution("Unresolved children");
    }

    @Evaluator
    static int process(BytesRef val) {
        return val.length;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ByteLength(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ByteLength::new, field());
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return new ByteLengthEvaluator.Factory(source(), toEvaluator.apply(field()));
    }
}
