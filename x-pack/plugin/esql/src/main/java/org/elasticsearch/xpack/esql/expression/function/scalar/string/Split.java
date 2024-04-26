/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.ql.InvalidArgumentException;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.function.scalar.BinaryScalarFunction;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.function.Function;

import static org.elasticsearch.xpack.esql.expression.EsqlTypeResolutions.isStringAndExact;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.SECOND;

/**
 * Splits a string on some delimiter into a multivalued string field.
 */
public class Split extends BinaryScalarFunction implements EvaluatorMapper {
    @FunctionInfo(
        returnType = "keyword",
        description = "Split a single valued string into multiple strings.",
        examples = @Example(file = "string", tag = "split")
    )
    public Split(
        Source source,
        @Param(
            name = "string",
            type = { "keyword", "text" },
            description = "String expression. If `null`, the function returns `null`."
        ) Expression str,
        @Param(
            name = "delim",
            type = { "keyword", "text" },
            description = "Delimiter. Only single byte delimiters are currently supported."
        ) Expression delim
    ) {
        super(source, str, delim);
    }

    @Override
    public DataType dataType() {
        return DataTypes.KEYWORD;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = isStringAndExact(left(), sourceText(), FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }

        return isStringAndExact(right(), sourceText(), SECOND);
    }

    @Override
    public boolean foldable() {
        return left().foldable() && right().foldable();
    }

    @Override
    public Object fold() {
        return EvaluatorMapper.super.fold();
    }

    @Evaluator(extraName = "SingleByte")
    static void process(
        BytesRefBlock.Builder builder,
        BytesRef str,
        @Fixed byte delim,
        @Fixed(includeInToString = false, build = true) BytesRef scratch
    ) {
        scratch.bytes = str.bytes;
        scratch.offset = str.offset;
        int end = str.offset + str.length;
        for (int i = str.offset; i < end; i++) {
            if (str.bytes[i] == delim) {
                scratch.length = i - scratch.offset;
                if (scratch.offset == str.offset) {
                    builder.beginPositionEntry();
                }
                builder.appendBytesRef(scratch);
                scratch.offset = i + 1;
            }
        }
        if (scratch.offset == str.offset) {
            // Delimiter not found, single valued
            builder.appendBytesRef(str);
            return;
        }
        scratch.length = str.length - (scratch.offset - str.offset);
        builder.appendBytesRef(scratch);
        builder.endPositionEntry();
    }

    @Evaluator(extraName = "Variable")
    static void process(
        BytesRefBlock.Builder builder,
        BytesRef str,
        BytesRef delim,
        @Fixed(includeInToString = false, build = true) BytesRef scratch
    ) {
        checkDelimiter(delim);
        process(builder, str, delim.bytes[delim.offset], scratch);
    }

    @Override
    protected BinaryScalarFunction replaceChildren(Expression newLeft, Expression newRight) {
        return new Split(source(), newLeft, newRight);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Split::new, left(), right());
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(Function<Expression, ExpressionEvaluator.Factory> toEvaluator) {
        var str = toEvaluator.apply(left());
        if (right().foldable() == false) {
            return new SplitVariableEvaluator.Factory(source(), str, toEvaluator.apply(right()), context -> new BytesRef());
        }
        BytesRef delim = (BytesRef) right().fold();
        checkDelimiter(delim);
        return new SplitSingleByteEvaluator.Factory(source(), str, delim.bytes[delim.offset], context -> new BytesRef());
    }

    private static void checkDelimiter(BytesRef delim) {
        if (delim.length != 1) {
            throw new InvalidArgumentException("delimiter must be single byte for now");
        }
    }
}
