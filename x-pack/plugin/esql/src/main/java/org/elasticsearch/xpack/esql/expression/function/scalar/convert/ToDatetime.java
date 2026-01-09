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
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.compute.ann.ConvertEvaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.ConfigurationFunction;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_NANOS;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSIGNED_LONG;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.DEFAULT_DATE_TIME_FORMATTER;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateTimeToLong;

public class ToDatetime extends AbstractConvertFunction implements ConfigurationFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "ToDatetime",
        ToDatetime::new
    );

    private static final Map<DataType, BuildFactory> STATIC_EVALUATORS = Map.ofEntries(
        Map.entry(DATETIME, (source, field) -> field),
        Map.entry(DATE_NANOS, ToDatetimeFromDateNanosEvaluator.Factory::new),
        Map.entry(LONG, (source, field) -> field),
        Map.entry(DOUBLE, ToLongFromDoubleEvaluator.Factory::new),
        Map.entry(UNSIGNED_LONG, ToLongFromUnsignedLongEvaluator.Factory::new),
        Map.entry(INTEGER, ToLongFromIntEvaluator.Factory::new), // CastIntToLongEvaluator would be a candidate, but not MV'd

        // Evaluators dynamically created in #factories()
        Map.entry(KEYWORD, (source, fieldEval) -> null),
        Map.entry(TEXT, (source, fieldEval) -> null)
    );

    private Map<DataType, BuildFactory> lazyEvaluators = null;

    private final Configuration configuration;

    @FunctionInfo(
        returnType = "date",
        description = """
            Converts an input value to a date value.
            A string will only be successfully converted if it’s respecting the format `yyyy-MM-dd'T'HH:mm:ss.SSS'Z'`.
            To convert dates in other formats, use <<esql-date_parse>>.""",
        note = "Note that when converting from nanosecond resolution to millisecond resolution with this function, the nanosecond date is "
            + "truncated, not rounded.",
        examples = {
            @Example(file = "date", tag = "to_datetime-str", explanation = """
                Note that in this example, the last value in the source multi-valued field has not been converted.
                The reason being that if the date format is not respected, the conversion will result in a `null` value.
                When this happens a _Warning_ header is added to the response.
                The header will provide information on the source of the failure:

                `"Line 1:112: evaluation of [TO_DATETIME(string)] failed, treating result as null. "Only first 20 failures recorded."`

                A following header will contain the failure reason and the offending value:

                `"java.lang.IllegalArgumentException: failed to parse date field [1964-06-02 00:00:00]
                with format [yyyy-MM-dd'T'HH:mm:ss.SSS'Z']"`
                """),
            @Example(
                description = """
                    If the input parameter is of a numeric type,
                    its value will be interpreted as milliseconds since the {wikipedia}/Unix_time[Unix epoch]. For example:""",
                file = "date",
                tag = "to_datetime-int"
            ) }
    )
    public ToDatetime(
        Source source,
        @Param(
            name = "field",
            type = { "date", "date_nanos", "keyword", "text", "double", "long", "unsigned_long", "integer" },
            description = "Input value. The input can be a single- or multi-valued column or an expression."
        ) Expression field,
        Configuration configuration
    ) {
        super(source, field);
        this.configuration = configuration;
    }

    private ToDatetime(StreamInput in) throws IOException {
        super(in);
        this.configuration = ((PlanStreamInput) in).configuration();
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected Map<DataType, BuildFactory> factories() {
        if (lazyEvaluators == null) {
            Map<DataType, BuildFactory> evaluators = new HashMap<>(STATIC_EVALUATORS);
            evaluators.putAll(
                Map.ofEntries(
                    Map.entry(
                        KEYWORD,
                        (source, fieldEval) -> new ToDatetimeFromStringEvaluator.Factory(
                            source,
                            fieldEval,
                            DEFAULT_DATE_TIME_FORMATTER.withZone(configuration.zoneId())
                        )
                    ),
                    Map.entry(
                        TEXT,
                        (source, fieldEval) -> new ToDatetimeFromStringEvaluator.Factory(
                            source,
                            fieldEval,
                            DEFAULT_DATE_TIME_FORMATTER.withZone(configuration.zoneId())
                        )
                    )
                )
            );
            lazyEvaluators = evaluators;
        }
        return lazyEvaluators;
    }

    @Override
    public DataType dataType() {
        return DATETIME;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ToDatetime(source(), newChildren.get(0), configuration);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ToDatetime::new, field(), configuration);
    }

    @ConvertEvaluator(extraName = "FromString", warnExceptions = { IllegalArgumentException.class })
    static long fromKeyword(BytesRef in, @Fixed DateFormatter formatter) {
        return dateTimeToLong(in.utf8ToString(), formatter);
    }

    @ConvertEvaluator(extraName = "FromDateNanos", warnExceptions = { IllegalArgumentException.class })
    static long fromDatenanos(long in) {
        return DateUtils.toMilliSeconds(in);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass(), children(), configuration);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) == false) {
            return false;
        }
        ToDatetime other = (ToDatetime) obj;

        return configuration.equals(other.configuration);
    }
}
