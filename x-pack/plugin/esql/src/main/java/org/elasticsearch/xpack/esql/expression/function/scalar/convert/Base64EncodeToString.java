/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.ann.ConvertEvaluator;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.ql.type.DataTypes.TEXT;

public class Base64EncodeToString extends AbstractConvertFunction implements EvaluatorMapper {

    private static final Map<DataType, BuildFactory> EVALUATORS = Map.ofEntries(
        Map.entry(KEYWORD, Base64EncodeToStringEvaluator.Factory::new),
        Map.entry(TEXT, Base64EncodeToStringEvaluator.Factory::new)
    );

    @FunctionInfo(
        returnType = "keyword",
        description = "Encode a string to a base64 string.",
        examples = @Example(file = "string", tag = "base64_encode")
    )
    public Base64EncodeToString(
        Source source,
        @Param(name = "string", type = { "keyword", "text" }, description = "A string.") Expression string
    ) {
        super(source, string);
    }

    @Override
    protected Map<DataType, BuildFactory> factories() {
        return EVALUATORS;
    }

    @Override
    public DataType dataType() {
        return KEYWORD;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Base64EncodeToString(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Base64EncodeToString::new, field());
    }

    @ConvertEvaluator()
    static BytesRef process(BytesRef input) {
        byte[] bytes = new byte[input.length];
        System.arraycopy(input.bytes, input.offset, bytes, 0, input.length);
        return new BytesRef(Base64.getEncoder().encode(bytes));
    }
}
