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
import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.compute.ann.ConvertEvaluator;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DataTypeConverter;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_NANOS;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSIGNED_LONG;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.DEFAULT_DATE_NANOS_FORMATTER;

public class ToDateNanos extends AbstractConvertFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "ToDateNanos",
        ToDateNanos::new
    );

    private static final Map<DataType, BuildFactory> EVALUATORS = Map.ofEntries(
        Map.entry(DATETIME, ToDateNanosFromDatetimeEvaluator.Factory::new),
        Map.entry(DATE_NANOS, (source, field) -> field),
        Map.entry(LONG, ToDateNanosFromLongEvaluator.Factory::new),
        Map.entry(KEYWORD, ToDateNanosFromStringEvaluator.Factory::new),
        Map.entry(TEXT, ToDateNanosFromStringEvaluator.Factory::new),
        Map.entry(DOUBLE, ToDateNanosFromDoubleEvaluator.Factory::new),
        Map.entry(UNSIGNED_LONG, ToLongFromUnsignedLongEvaluator.Factory::new)
        /*
         NB: not including an integer conversion, because max int in nanoseconds is like 2 seconds after epoch, and it seems more likely
         a user who tries to convert an int to a nanosecond date has made a mistake that we should catch that at parse time.
         TO_DATE_NANOS(TO_LONG(intVal)) is still possible if someone really needs to do this.
         */
    );

    @FunctionInfo(
        returnType = "date_nanos",
        description = "Converts an input to a nanosecond-resolution date value (aka date_nanos).",
        note = "The range for date nanos is 1970-01-01T00:00:00.000000000Z to 2262-04-11T23:47:16.854775807Z, attempting to convert "
            + "values outside of that range will result in null with a warning.  Additionally, integers cannot be converted into date "
            + "nanos, as the range of integer nanoseconds only covers about 2 seconds after epoch.",
        examples = { @Example(file = "date_nanos", tag = "to_date_nanos") }
    )
    public ToDateNanos(
        Source source,
        @Param(
            name = "field",
            type = { "date", "date_nanos", "keyword", "text", "double", "long", "unsigned_long" },
            description = "Input value. The input can be a single- or multi-valued column or an expression."
        ) Expression field
    ) {
        super(source, field);
    }

    protected ToDateNanos(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public DataType dataType() {
        return DATE_NANOS;
    }

    @Override
    protected Map<DataType, BuildFactory> factories() {
        return EVALUATORS;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ToDateNanos(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ToDateNanos::new, field());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @ConvertEvaluator(extraName = "FromLong", warnExceptions = { IllegalArgumentException.class })
    static long fromLong(long in) {
        if (in < 0L) {
            throw new IllegalArgumentException("Nanosecond dates before 1970-01-01T00:00:00.000Z are not supported.");
        }
        return in;
    }

    @ConvertEvaluator(extraName = "FromDouble", warnExceptions = { IllegalArgumentException.class, InvalidArgumentException.class })
    static long fromDouble(double in) {
        if (in < 0d) {
            throw new IllegalArgumentException("Nanosecond dates before 1970-01-01T00:00:00.000Z are not supported.");
        }
        return DataTypeConverter.safeDoubleToLong(in);
    }

    @ConvertEvaluator(extraName = "FromString", warnExceptions = { IllegalArgumentException.class })
    static long fromKeyword(BytesRef in) {
        Instant parsed = DateFormatters.from(DEFAULT_DATE_NANOS_FORMATTER.parse(in.utf8ToString())).toInstant();
        return DateUtils.toLong(parsed);
    }

    @ConvertEvaluator(extraName = "FromDatetime", warnExceptions = { IllegalArgumentException.class })
    static long fromDatetime(long in) {
        return DateUtils.toNanoSeconds(in);
    }
}
