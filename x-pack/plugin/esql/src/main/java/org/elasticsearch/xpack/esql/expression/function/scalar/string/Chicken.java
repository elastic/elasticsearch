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
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.compute.ann.Fixed.Scope.THREAD_LOCAL;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;

/**
 * A fun Easter egg function that wraps text in ASCII art of a chicken saying something,
 * similar to the classic "cowsay" command.
 */
public class Chicken extends EsqlScalarFunction implements OptionalArgument {
    public static final String CHICKEN_EMOJI = "\uD83D\uDC14";

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Chicken", Chicken::new);

    private final Expression message;
    private final Expression style;

    @FunctionInfo(returnType = "keyword", description = """
        Returns a string with the input text wrapped in ASCII art of a chicken saying the message.
        This is an Easter egg function inspired by the classic "cowsay" command.""")
    public Chicken(
        Source source,
        @Param(name = "message", type = { "keyword", "text" }, description = "The message for the chicken to say.") Expression message,
        @Param(
            name = "style",
            type = { "keyword", "text" },
            optional = true,
            description = "Optional chicken style. Available styles: ordinary, early_state, laying, thinks_its_a_duck, "
                + "smoking_a_pipe, soup, racing, stoned, whistling. If not specified, a random style is chosen."
        ) Expression style
    ) {
        super(source, Arrays.asList(message, style == null ? randomStyleLiteral(source) : style));
        this.message = message;
        this.style = style == null ? randomStyleLiteral(source) : style;
    }

    /**
     * Creates a literal expression with a randomly selected chicken style.
     * This is called at construction time so the style is fixed for this expression.
     */
    private static Literal randomStyleLiteral(Source source) {
        String styleName = ChickenArtBuilder.random().name().toLowerCase(java.util.Locale.ROOT);
        return new Literal(source, new BytesRef(styleName), DataType.KEYWORD);
    }

    private Chicken(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(message);
        out.writeNamedWriteable(style);
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
    public boolean foldable() {
        return message.foldable() && style.foldable();
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = isString(message, sourceText(), FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }

        resolution = isString(style, sourceText(), SECOND);
        if (resolution.unresolved()) {
            return resolution;
        }

        // If style is a literal, validate it
        if (style.foldable()) {
            Object foldedStyle = style.fold(FoldContext.small());
            if (foldedStyle != null) {
                String styleName = foldedStyle instanceof BytesRef br ? br.utf8ToString() : foldedStyle.toString();
                if (ChickenArtBuilder.fromName(styleName) == null) {
                    return new TypeResolution(
                        "Unknown chicken style [" + styleName + "]. Available styles: " + ChickenArtBuilder.availableStyles()
                    );
                }
            }
        }

        return TypeResolution.TYPE_RESOLVED;
    }

    @Evaluator
    static BytesRef process(
        @Fixed(includeInToString = false, scope = THREAD_LOCAL) BreakingBytesRefBuilder scratch,
        BytesRef message,
        BytesRef style
    ) {
        scratch.clear();
        ChickenArtBuilder chicken = ChickenArtBuilder.fromName(style);
        if (chicken == null) {
            // Fall back to random if style not found (shouldn't happen due to validation)
            chicken = ChickenArtBuilder.random();
        }
        chicken.buildChickenSay(scratch, message);
        return scratch.bytesRefView();
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Chicken(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Chicken::new, message, style);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return new ChickenEvaluator.Factory(
            source(),
            context -> new BreakingBytesRefBuilder(context.breaker(), "chicken"),
            toEvaluator.apply(message),
            toEvaluator.apply(style)
        );
    }
}
