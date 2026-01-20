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
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.MapParam;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static java.util.Map.entry;
import static org.elasticsearch.compute.ann.Fixed.Scope.THREAD_LOCAL;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;
import static org.elasticsearch.xpack.esql.expression.function.Options.resolve;

/**
 * A fun Easter egg function that wraps text in ASCII art of a chicken saying something,
 * similar to the classic "cowsay" command.
 */
public class Chicken extends EsqlScalarFunction implements OptionalArgument {
    public static final String CHICKEN_EMOJI = "\uD83D\uDC14";

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Chicken", Chicken::new);

    private static final String STYLE_OPTION = "style";
    private static final String WIDTH_OPTION = "width";
    private static final String DEFAULT_STYLE = "ordinary";
    private static final int DEFAULT_WIDTH = 40;

    public static final Map<String, DataType> ALLOWED_OPTIONS = Map.ofEntries(
        entry(STYLE_OPTION, DataType.KEYWORD),
        entry(WIDTH_OPTION, DataType.INTEGER)
    );

    private final Expression message;
    private final Expression options;

    @FunctionInfo(returnType = "keyword", description = """
        Returns a string with the input text wrapped in ASCII art of a chicken saying the message.
        This is an Easter egg function inspired by the classic "cowsay" command.""")
    public Chicken(
        Source source,
        @Param(name = "message", type = { "keyword", "text" }, description = "The message for the chicken to say.") Expression message,
        @MapParam(
            name = "options",
            description = "Optional settings for the chicken output.",
            optional = true,
            params = {
                @MapParam.MapParamEntry(
                    name = "style",
                    type = "keyword",
                    description = "Chicken style. Available styles: ordinary, early_state, laying, thinks_its_a_duck, "
                        + "smoking_a_pipe, soup, racing, stoned, realistic, whistling. Defaults to ordinary.",
                    valueHint = {
                        "ordinary",
                        "early_state",
                        "laying",
                        "thinks_its_a_duck",
                        "smoking_a_pipe",
                        "soup",
                        "racing",
                        "stoned",
                        "realistic",
                        "whistling" }
                ),
                @MapParam.MapParamEntry(
                    name = "width",
                    type = "integer",
                    description = "Maximum width of the speech bubble. Defaults to 40, maximum is 76.",
                    valueHint = { "40", "60", "76" }
                ) }
        ) Expression options
    ) {
        super(source, options == null ? List.of(message) : List.of(message, options));
        this.message = message;
        this.options = options;
    }

    private Chicken(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readOptionalNamedWriteable(Expression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(message);
        out.writeOptionalNamedWriteable(options);
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
        return message.foldable() && (options == null || options.foldable());
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        return isString(message, sourceText(), FIRST).and(() -> resolve(options, source(), SECOND, ALLOWED_OPTIONS))
            .and(this::validateOptions);
    }

    private TypeResolution validateOptions() {
        if (options == null) {
            return TypeResolution.TYPE_RESOLVED;
        }

        // Validate style option if provided
        String styleName = extractStringOption(STYLE_OPTION, DEFAULT_STYLE);
        if (ChickenArtBuilder.fromName(styleName) == null) {
            return new TypeResolution(
                "Unknown chicken style [" + styleName + "]. Available styles: " + ChickenArtBuilder.availableStyles()
            );
        }

        // Validate width option if provided
        int width = extractIntegerOption(WIDTH_OPTION, DEFAULT_WIDTH);
        if (width <= 0) {
            return new TypeResolution("'" + WIDTH_OPTION + "' option must be a positive integer, found [" + width + "]");
        }

        return TypeResolution.TYPE_RESOLVED;
    }

    private String extractStringOption(String optionName, String defaultValue) {
        if (options == null) {
            return defaultValue;
        }
        MapExpression optionsMap = (MapExpression) options;
        Expression expr = optionsMap.keyFoldedMap().get(optionName);
        if (expr == null) {
            return defaultValue;
        }
        Object value = expr.fold(FoldContext.small());
        if (value == null) {
            return defaultValue;
        }
        return value instanceof BytesRef br ? br.utf8ToString() : value.toString();
    }

    private int extractIntegerOption(String optionName, int defaultValue) {
        if (options == null) {
            return defaultValue;
        }
        MapExpression optionsMap = (MapExpression) options;
        Expression expr = optionsMap.keyFoldedMap().get(optionName);
        if (expr == null) {
            return defaultValue;
        }
        Object value = expr.fold(FoldContext.small());
        return value != null ? ((Number) value).intValue() : defaultValue;
    }

    @Evaluator
    static BytesRef process(
        @Fixed(includeInToString = false, scope = THREAD_LOCAL) BreakingBytesRefBuilder scratch,
        BytesRef message,
        @Fixed ChickenArtBuilder chickenStyle,
        @Fixed int width
    ) {
        scratch.clear();
        chickenStyle.buildChickenSay(scratch, message, width);
        return scratch.bytesRefView();
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Chicken(source(), newChildren.get(0), newChildren.size() > 1 ? newChildren.get(1) : null);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Chicken::new, message, options);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        String styleName = extractStringOption(STYLE_OPTION, DEFAULT_STYLE);
        ChickenArtBuilder chickenStyle = ChickenArtBuilder.fromName(styleName);
        if (chickenStyle == null) {
            chickenStyle = ChickenArtBuilder.ORDINARY;
        }
        int width = extractIntegerOption(WIDTH_OPTION, DEFAULT_WIDTH);

        return new ChickenEvaluator.Factory(
            source(),
            context -> new BreakingBytesRefBuilder(context.breaker(), "chicken"),
            toEvaluator.apply(message),
            chickenStyle,
            width
        );
    }
}
