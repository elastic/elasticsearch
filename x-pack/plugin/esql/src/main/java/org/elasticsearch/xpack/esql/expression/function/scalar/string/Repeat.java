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
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.common.unit.ByteSizeUnit.MB;
import static org.elasticsearch.compute.ann.Fixed.Scope.THREAD_LOCAL;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

public class Repeat extends EsqlScalarFunction implements OptionalArgument {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Repeat", Repeat::new);

    static final long MAX_REPEATED_LENGTH = MB.toBytes(1);

    private final Expression str;
    private final Expression number;

    @FunctionInfo(
        returnType = "keyword",
        description = "Returns a string constructed by concatenating `string` with itself the specified `number` of times.",
        examples = @Example(file = "string", tag = "repeat")
    )
    public Repeat(
        Source source,
        @Param(name = "string", type = { "keyword", "text" }, description = "String expression.") Expression str,
        @Param(name = "number", type = { "integer" }, description = "Number times to repeat.") Expression number
    ) {
        super(source, Arrays.asList(str, number));
        this.str = str;
        this.number = number;
    }

    private Repeat(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(str);
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

        TypeResolution resolution = isString(str, sourceText(), FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }

        return isType(number, dt -> dt == DataType.INTEGER, sourceText(), SECOND, "integer");
    }

    @Override
    public boolean foldable() {
        return str.foldable() && number.foldable();
    }

    @Evaluator(extraName = "Constant", warnExceptions = { IllegalArgumentException.class })
    static BytesRef processConstantNumber(
        @Fixed(includeInToString = false, scope = THREAD_LOCAL) BreakingBytesRefBuilder scratch,
        BytesRef str,
        @Fixed int number
    ) {
        return processInner(scratch, str, number);
    }

    @Evaluator(warnExceptions = { IllegalArgumentException.class })
    static BytesRef process(
        @Fixed(includeInToString = false, scope = THREAD_LOCAL) BreakingBytesRefBuilder scratch,
        BytesRef str,
        int number
    ) {
        if (number < 0) {
            throw new IllegalArgumentException("Number parameter cannot be negative, found [" + number + "]");
        }
        return processInner(scratch, str, number);
    }

    static BytesRef processInner(BreakingBytesRefBuilder scratch, BytesRef str, int number) {
        int repeatedLen = str.length * number;
        if (repeatedLen > MAX_REPEATED_LENGTH) {
            throw new IllegalArgumentException(
                "Creating repeated strings with more than [" + MAX_REPEATED_LENGTH + "] bytes is not supported"
            );
        }
        scratch.grow(repeatedLen);
        scratch.clear();
        for (int i = 0; i < number; ++i) {
            scratch.append(str);
        }
        return scratch.bytesRefView();
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Repeat(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Repeat::new, str, number);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        ExpressionEvaluator.Factory strExpr = toEvaluator.apply(str);

        if (number.foldable()) {
            int num = (int) number.fold(toEvaluator.foldCtx());
            if (num < 0) {
                throw new IllegalArgumentException("Number parameter cannot be negative, found [" + number + "]");
            }
            return new RepeatConstantEvaluator.Factory(
                source(),
                context -> new BreakingBytesRefBuilder(context.breaker(), "repeat"),
                strExpr,
                num
            );
        }

        ExpressionEvaluator.Factory numberExpr = toEvaluator.apply(number);
        return new RepeatEvaluator.Factory(
            source(),
            context -> new BreakingBytesRefBuilder(context.breaker(), "repeat"),
            strExpr,
            numberExpr
        );
    }

    Expression str() {
        return str;
    }

    Expression number() {
        return number;
    }
}
