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
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.planner.Mappable;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.function.scalar.BinaryScalarFunction;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isStringAndExact;

/**
 * Splits a string on some delimiter into a multivalued string field.
 */
public class Split extends BinaryScalarFunction implements Mappable {
    public Split(Source source, Expression str, Expression delim) {
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
        return Mappable.super.fold();
    }

    @Evaluator(extraName = "SingleByte")
    static void process(
        BytesRefBlock.Builder builder,
        BytesRef str,
        @Fixed byte delim,
        @Fixed(includeInToString = false) BytesRef scratch
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
    static void process(BytesRefBlock.Builder builder, BytesRef str, BytesRef delim, @Fixed(includeInToString = false) BytesRef scratch) {
        if (delim.length != 1) {
            throw new QlIllegalArgumentException("delimiter must be single byte for now");
        }
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
    public Supplier<EvalOperator.ExpressionEvaluator> toEvaluator(
        Function<Expression, Supplier<EvalOperator.ExpressionEvaluator>> toEvaluator
    ) {
        Supplier<EvalOperator.ExpressionEvaluator> str = toEvaluator.apply(left());
        if (right().foldable() == false) {
            Supplier<EvalOperator.ExpressionEvaluator> delim = toEvaluator.apply(right());
            return () -> new SplitVariableEvaluator(str.get(), delim.get(), new BytesRef());
        }
        BytesRef delim = (BytesRef) right().fold();
        if (delim.length != 1) {
            throw new QlIllegalArgumentException("for now delimiter must be a single byte");
        }
        return () -> new SplitSingleByteEvaluator(str.get(), delim.bytes[delim.offset], new BytesRef());
    }
}
