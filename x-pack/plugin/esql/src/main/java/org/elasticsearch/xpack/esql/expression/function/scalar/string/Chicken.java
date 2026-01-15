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
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.UnaryScalarFunction;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.compute.ann.Fixed.Scope.THREAD_LOCAL;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;

/**
 * A fun Easter egg function that wraps text in ASCII art of a chicken saying something,
 * similar to the classic "cowsay" command.
 */
public class Chicken extends UnaryScalarFunction {
    public static final String CHICKEN_EMOJI = "\uD83D\uDC14";

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Chicken", Chicken::new);

    @FunctionInfo(returnType = "keyword", description = """
        Returns a string with the input text wrapped in ASCII art of a chicken saying the message.
        This is an Easter egg function inspired by the classic "cowsay" command.""")
    public Chicken(
        Source source,
        @Param(name = "message", type = { "keyword", "text" }, description = "The message for the chicken to say.") Expression message
    ) {
        super(source, message);
    }

    private Chicken(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field());
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

        return isString(field(), sourceText(), DEFAULT);
    }

    @Evaluator
    static BytesRef process(@Fixed(includeInToString = false, scope = THREAD_LOCAL) BreakingBytesRefBuilder scratch, BytesRef message) {
        String text = message.utf8ToString();
        scratch.clear();
        ChickenArtBuilder.random().buildChickenSay(scratch, text);
        return scratch.bytesRefView();
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Chicken(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Chicken::new, field());
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return new ChickenEvaluator.Factory(
            source(),
            context -> new BreakingBytesRefBuilder(context.breaker(), "chicken"),
            toEvaluator.apply(field())
        );
    }
}
