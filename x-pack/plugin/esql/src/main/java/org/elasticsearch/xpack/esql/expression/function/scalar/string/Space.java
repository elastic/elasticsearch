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
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.common.unit.ByteSizeUnit.MB;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNumeric;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isWholeNumber;

public class Space extends UnaryScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Space", Space::new);

    static final long MAX_LENGTH = MB.toBytes(1);

    private final Expression number;

    @FunctionInfo(
        returnType = "keyword",
        description = "Returns a string of `number` spaces.",
        examples = @Example(file = "string", tag = "space")
    )
    public Space(
        Source source,
        @Param(name = "number", type = { "integer" }, description = "Number of spaces in result.") Expression number
    ) {
        super(source, number);
        this.number = number;
    }

    private Space(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(number);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return DataType.KEYWORD;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        return isType(number, dt -> dt == DataType.INTEGER, sourceText(), DEFAULT, "integer");
    }

    @Override
    public boolean foldable() {
        return number.foldable();
    }

    @Evaluator(extraName = "Constant")
    static BytesRef processConstant(@Fixed(includeInToString = false, build = true) BreakingBytesRefBuilder scratch, @Fixed int number) {
        return processInner(scratch, number);
    }

    @Evaluator(warnExceptions = { IllegalArgumentException.class })
    static BytesRef process(@Fixed(includeInToString = false, build = true) BreakingBytesRefBuilder scratch, int number) {
        checkNumber(number);
        return processInner(scratch, number);
    }

    static void checkNumber(int number) {
        if (number < 0) {
            throw new IllegalArgumentException("Number parameter cannot be negative, found [" + number + "]");
        }
        if (number > MAX_LENGTH) {
            throw new IllegalArgumentException("Creating strings longer than [" + MAX_LENGTH + "] bytes is not supported");
        }
    }

    static BytesRef processInner(BreakingBytesRefBuilder scratch, int number) {
        scratch.grow(number);
        scratch.clear();
        for (int i = 0; i < number; ++i) {
            scratch.append((byte) ' ');
        }
        return scratch.bytesRefView();
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Space(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Space::new, number);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(Function<Expression, ExpressionEvaluator.Factory> toEvaluator) {
        if (number.foldable()) {
            int num = (int) number.fold();
            checkNumber(num);
            return new SpaceConstantEvaluator.Factory(source(), context -> new BreakingBytesRefBuilder(context.breaker(), "space"), num);
        }

        ExpressionEvaluator.Factory numberExpr = toEvaluator.apply(number);
        return new SpaceEvaluator.Factory(source(), context -> new BreakingBytesRefBuilder(context.breaker(), "space"), numberExpr);
    }

    Expression number() {
        return number;
    }
}
