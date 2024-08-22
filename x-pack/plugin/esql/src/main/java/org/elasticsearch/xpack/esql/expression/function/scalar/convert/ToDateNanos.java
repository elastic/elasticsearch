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
import org.elasticsearch.compute.ann.ConvertEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_NANOS;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
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
        Map.entry(DATE_NANOS, (field, source) -> field),
        Map.entry(LONG, (field, source) -> field),
        Map.entry(KEYWORD, ToDateNanosFromStringEvaluator.Factory::new),
        Map.entry(TEXT, ToDateNanosFromStringEvaluator.Factory::new),
        Map.entry(DOUBLE, ToLongFromDoubleEvaluator.Factory::new),
        Map.entry(UNSIGNED_LONG, ToLongFromUnsignedLongEvaluator.Factory::new)
        /*
         NB: not including an integer conversion, because max int in nanoseconds is like 2 seconds after epoch, and it seems more likely
         a user who tries to convert an int to a nanosecond date has made a mistake that we should catch that at parse time.
         TO_DATE_NANOS(TO_LONG(intVal)) is still possible if someone really needs to do this.
         */
    );

    @FunctionInfo(returnType = "date_nanos", description = """
        Converts an input to a nanosecond-resolution date value (aka date_nanos).
        `yyyy-MM-dd'T'HH:mm:ss.SSS'Z'
        """)
    public ToDateNanos(
        Source source,
        @Param(
            name = "field",
            type = { "date", "date_nanos", "keyword", "text", "double", "long", "unsigned_long", "integer" },
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

    @ConvertEvaluator(extraName = "FromString", warnExceptions = { IllegalArgumentException.class })
    static long fromKeyword(BytesRef in) {
        Instant parsed = DateFormatters.from(DEFAULT_DATE_NANOS_FORMATTER.parse(in.utf8ToString())).toInstant();
        long nanos = parsed.getEpochSecond();
        try {
            nanos = Math.multiplyExact(nanos, 1_000_000_000) + parsed.getNano();
        } catch (ArithmeticException e) {
            // This seems like a more useful error message than "Long Overflow"
            throw new IllegalArgumentException("cannot create nanosecond dates after 2262-04-11T23:47:16.854775807Z");
        }
        return nanos;
    }

    @ConvertEvaluator(extraName = "FromDatetime", warnExceptions = { IllegalArgumentException.class })
    static long fromDatetime(long in) {
        try {
            return Math.multiplyExact(in, 1_000_000L);
        } catch (ArithmeticException e) {
            // This seems like a more useful error message than "Long Overflow"
            throw new IllegalArgumentException("cannot create nanosecond dates after 2262-04-11T23:47:16.854775807Z");
        }
    }

}
