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
import org.elasticsearch.xpack.esql.capabilities.Validatable;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.io.IOException;
import java.time.temporal.TemporalAmount;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.TIME_DURATION;
import static org.elasticsearch.xpack.esql.core.type.DataType.isString;
import static org.elasticsearch.xpack.esql.expression.Validations.isFoldable;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.INVALID_INTERVAL_ERROR;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.TIME_DURATIONS;

public class ToTimeDuration extends AbstractConvertFunction implements Validatable {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "ToTimeDuration",
        ToTimeDuration::new
    );

    @FunctionInfo(
        returnType = "time_duration",
        description = "Converts an input value into a `time_duration` value.",
        examples = @Example(file = "convert", tag = "castToTimeDuration")
    )
    public ToTimeDuration(
        Source source,
        @Param(
            name = "field",
            type = { "time_duration", "keyword", "text" },
            description = "Input value. The input is a valid constant time duration expression."
        ) Expression v
    ) {
        super(source, v);
    }

    private ToTimeDuration(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class));
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected final TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        TypeResolution typeResolution = isType(
            field(),
            dt -> isString(dt) || dt == TIME_DURATION,
            sourceText(),
            null,
            "string or time_duration"
        );
        if (field().foldable() && field() instanceof Literal && isString(field().dataType())) {
            Object value = field().fold();
            if (value != null) {
                if (value instanceof BytesRef b) {
                    value = b.utf8ToString();
                }
                if (isValidInterval(value.toString()) == false) {
                    typeResolution = new TypeResolution(
                        LoggerMessageFormat.format(null, INVALID_INTERVAL_ERROR, sourceText(), TIME_DURATIONS, field().fold())
                    );
                }
            }
        }
        return typeResolution;
    }

    @Override
    protected Map<DataType, BuildFactory> factories() {
        return Map.of();
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
        if (field().foldable()) {
            Object v = field().fold();
            if (v instanceof BytesRef b) {
                if (isValidInterval(b.utf8ToString()) == false) {
                    throw new IllegalArgumentException(
                        LoggerMessageFormat.format(null, INVALID_INTERVAL_ERROR, sourceText(), TIME_DURATIONS, b.utf8ToString())
                    );
                }
                return EsqlDataTypeConverter.parseTemporalAmount(b.utf8ToString(), TIME_DURATION);
            } else if (v instanceof TemporalAmount) {
                return v;
            }
        }

        throw new IllegalArgumentException(
            LoggerMessageFormat.format(null, "argument of [{}] must be a constant, received [{}]", sourceText(), Expressions.name(field()))
        );
    }

    private boolean isValidInterval(String interval) {
        try {
            String[] input = interval.toUpperCase(Locale.ROOT).stripLeading().stripTrailing().split("\\s+");
            if (input.length != 2 || TIME_DURATIONS.contains(EsqlDataTypeConverter.INTERVALS.valueOf(input[1])) == false) {
                return false;
            }
            Integer.parseInt(input[0].toString());
        } catch (IllegalArgumentException e) {
            return false;
        }
        return true;
    }

    @Override
    public void validate(Failures failures) {
        String operation = sourceText();
        failures.add(isFoldable(field(), operation, null));
    }
}
