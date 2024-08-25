/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.core.type.DataType.TIME_DURATION;
import static org.elasticsearch.xpack.esql.core.type.DataType.isString;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.TIME_DURATIONS;

public class ToTimeDuration extends AbstractConvertFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "ToTimeDuration",
        ToTimeDuration::new
    );

    private static final Map<DataType, BuildFactory> EVALUATORS = Map.ofEntries(
        Map.entry(TIME_DURATION, (fieldEval, source) -> fieldEval),
        Map.entry(KEYWORD, (fieldEval, source) -> fieldEval),
        Map.entry(TEXT, (fieldEval, source) -> fieldEval)
    );

    private static final String INVALID_INTERVAL_ERROR = "Invalid interval value in [{}], expected integer followed by one of "
        + TIME_DURATIONS
        + " but got [{}]";

    @FunctionInfo(
        returnType = "time_duration",
        description = "Converts a string into a `time_duration` value.",
        examples = @Example(file = "convert", tag = "castToTimeDuration")
    )
    public ToTimeDuration(
        Source source,
        @Param(name = "string", type = { "time_duration", "keyword", "text" }, description = "A string.") Expression v
    ) {
        super(source, v);
    }

    private ToTimeDuration(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected Map<DataType, BuildFactory> factories() {
        return EVALUATORS;
    }

    @Override
    protected final TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        TypeResolution typeResolution = isType(
            field(),
            factories()::containsKey,
            sourceText(),
            FIRST,
            supportedTypesNames(supportedTypes())
        );
        if (field().foldable()) {
            if (isString(field().dataType()) && isValidInterval(field().fold().toString()) == false) {
                typeResolution = new TypeResolution(LoggerMessageFormat.format(null, INVALID_INTERVAL_ERROR, sourceText(), field().fold()));
            }
        }
        return typeResolution;
    }

    @Override
    public DataType dataType() {
        return TIME_DURATION;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ToTimeDuration(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ToTimeDuration::new, field());
    }

    @Override
    public final Object fold() {
        if (field() instanceof Literal l) {
            Object v = l.value();
            if (v instanceof BytesRef b) {
                if (isValidInterval(b.utf8ToString()) == false) {
                    throw new IllegalArgumentException(
                        LoggerMessageFormat.format(null, INVALID_INTERVAL_ERROR, sourceText(), b.utf8ToString())
                    );
                }
                return EsqlDataTypeConverter.parseTemporalAmount(b.utf8ToString(), TIME_DURATION);
            }
        } else if (field() instanceof ToTimeDuration && field().foldable()) {
            return field().fold();
        }
        throw new IllegalArgumentException(
            LoggerMessageFormat.format(
                null,
                "{}argument of [{}] must be a constant, received [{}]",
                FIRST.name().toLowerCase(Locale.ROOT) + " ",
                sourceText(),
                Expressions.name(field())
            )
        );
    }

    private boolean isValidInterval(String interval) {
        String[] input = interval.toLowerCase(Locale.ROOT).stripLeading().stripTrailing().split("\\s+");
        if (input.length != 2 || TIME_DURATIONS.contains(input[1]) == false) {
            return false;
        }
        try {
            Integer.parseInt(input[0].toString());
        } catch (NumberFormatException e) {
            return false;
        }
        return true;
    }
}
