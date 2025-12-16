/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.util.LocaleUtils;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.MapParam;
import org.elasticsearch.xpack.esql.expression.function.Options;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.TwoOptionalArguments;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.time.ZoneId;
import java.time.zone.ZoneRulesException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static java.util.Map.entry;
import static org.elasticsearch.common.time.DateFormatter.forPattern;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isMapExpression;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.expression.EsqlTypeResolutions.isStringAndExact;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.DEFAULT_DATE_TIME_FORMATTER;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateTimeToLong;

public class DateParse extends EsqlScalarFunction implements TwoOptionalArguments {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "DateParse",
        DateParse::new
    );

    private static final String TIME_ZONE_PARAM_NAME = "time_zone";
    private static final String LOCALE_PARAM_NAME = "locale";

    private final Expression first;
    private final Expression second;
    private final Expression third;

    @FunctionInfo(
        returnType = "date",
        description = "Returns a date by parsing the second argument using the format specified in the first argument.",
        examples = @Example(file = "docs", tag = "dateParse")
    )
    public DateParse(
        Source source,
        @Param(name = "datePattern", type = { "keyword", "text" }, description = """
            The date format. Refer to the
            {javadoc14}/java.base/java/time/format/DateTimeFormatter.html[`DateTimeFormatter` documentation] for the syntax.
            If `null`, the function returns `null`.""", optional = true) Expression first,
        @Param(
            name = "dateString",
            type = { "keyword", "text" },
            description = "Date expression as a string. If `null` or an empty string, the function returns `null`."
        ) Expression second,
        @MapParam(
            name = "options",
            params = {
                @MapParam.MapParamEntry(
                    name = TIME_ZONE_PARAM_NAME,
                    type = "keyword",
                    valueHint = { "standard" },
                    description = "Coordinated Universal Time (UTC) offset or IANA time zone used to convert date values in the "
                        + "query string to UTC."
                ),
                @MapParam.MapParamEntry(
                    name = LOCALE_PARAM_NAME,
                    type = "keyword",
                    valueHint = { "standard" },
                    description = "The locale to use when parsing the date, relevant when parsing month names or week days."
                ) },
            description = "(Optional) Additional options for date parsing, specifying time zone and locale "
                + "as <<esql-function-named-params,function named parameters>>.",
            optional = true
        ) Expression third
    ) {
        super(source, fields(first, second, third));
        this.first = first;
        this.second = second;
        this.third = third;
    }

    private static List<Expression> fields(Expression first, Expression second, Expression third) {
        List<Expression> list = new ArrayList<>(3);
        list.add(first);
        if (second != null) {
            list.add(second);
        }
        if (third != null) {
            list.add(third);
        }
        return list;
    }

    private Expression field() {
        // If second is a MapExpression, then: first=date, second=options, third=null
        // If second is not a MapExpression and second exists, then: first=pattern, second=date, third=options
        // If only first exists, then: first=date, second=null, third=null
        if (second instanceof MapExpression) {
            return first;
        }
        return second != null ? second : first;
    }

    private Expression format() {
        // If second is a MapExpression, then: first=date, second=options, third=null (no format)
        // If second is not a MapExpression and second exists, then: first=pattern, second=date, third=options
        // If only first exists, then: first=date, second=null, third=null (no format)
        if (second instanceof MapExpression || second == null) {
            return null;
        }
        return first;
    }

    private Expression options() {
        // If second is a MapExpression, then: first=date, second=options, third=null
        // If second is not a MapExpression and third exists, then: first=pattern, second=date, third=options
        if (second instanceof MapExpression) {
            return second;
        }
        return third;
    }

    private DateParse(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readOptionalNamedWriteable(Expression.class),
            in.readOptionalNamedWriteable(Expression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(children().get(0));
        out.writeOptionalNamedWriteable(children().size() > 1 ? children().get(1) : null);
        out.writeOptionalNamedWriteable(children().size() > 2 ? children().get(2) : null);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return DataType.DATETIME;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution;
        Expression format = format();
        if (format != null) {
            resolution = isStringAndExact(format, sourceText(), FIRST);
            if (resolution.unresolved()) {
                return resolution;
            }
        }

        Expression field = field();
        resolution = isString(field, sourceText(), format != null ? SECOND : FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }

        // Validate options parameter if present
        Expression options = options();
        if (options != null) {
            resolution = isMapExpression(options, sourceText(), THIRD);
            if (resolution.unresolved()) {
                return resolution;
            }
        }

        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public boolean foldable() {
        Expression field = field();
        Expression format = format();
        return field.foldable() && (format == null || format.foldable());
    }

    @Evaluator(extraName = "Constant", warnExceptions = { IllegalArgumentException.class })
    public static long process(BytesRef val, @Fixed DateFormatter formatter) throws IllegalArgumentException {
        return dateTimeToLong(val.utf8ToString(), formatter);
    }

    @Evaluator(warnExceptions = { IllegalArgumentException.class })
    static long process(BytesRef val, BytesRef formatter) throws IllegalArgumentException {
        return dateTimeToLong(val.utf8ToString(), toFormatter(formatter));
    }

    public static final Map<String, DataType> ALLOWED_OPTIONS = Map.ofEntries(
        entry(TIME_ZONE_PARAM_NAME, KEYWORD),
        entry(LOCALE_PARAM_NAME, KEYWORD)
    );

    private Map<String, Object> parseOptions() throws InvalidArgumentException {
        Map<String, Object> matchOptions = new HashMap<>();
        Expression options = options();
        if (options == null) {
            return matchOptions;
        }

        Options.populateMap((MapExpression) options, matchOptions, source(), THIRD, ALLOWED_OPTIONS);
        return matchOptions;
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        Expression field = field();
        Expression format = format();
        ExpressionEvaluator.Factory fieldEvaluator = toEvaluator.apply(field);
        if (format == null) {
            return new DateParseConstantEvaluator.Factory(source(), fieldEvaluator, DEFAULT_DATE_TIME_FORMATTER);
        }
        if (DataType.isString(format.dataType()) == false) {
            throw new IllegalArgumentException("unsupported data type for date_parse [" + format.dataType() + "]");
        }
        var parsedOptions = this.parseOptions();
        String localeAsString = (String) parsedOptions.get(LOCALE_PARAM_NAME);
        Locale locale = localeAsString == null ? null : LocaleUtils.parse(localeAsString);

        String timezoneAsString = (String) parsedOptions.get(TIME_ZONE_PARAM_NAME);
        ZoneId timezone = null;
        try {
            if (timezoneAsString != null) {
                timezone = ZoneId.of(timezoneAsString);
            }
        } catch (ZoneRulesException e) {
            throw new IllegalArgumentException("unsupported timezone [" + timezoneAsString + "]");
        }

        if (format.foldable()) {
            try {
                DateFormatter formatter = toFormatter(format.fold(toEvaluator.foldCtx()));
                if (locale != null) {
                    formatter = formatter.withLocale(locale);
                }
                if (timezone != null) {
                    formatter = formatter.withZone(timezone);
                }
                return new DateParseConstantEvaluator.Factory(source(), fieldEvaluator, formatter);
            } catch (IllegalArgumentException e) {
                throw new InvalidArgumentException(e, "invalid date pattern for [{}]: {}", sourceText(), e.getMessage());
            }
        }
        ExpressionEvaluator.Factory formatEvaluator = toEvaluator.apply(format);
        return new DateParseEvaluator.Factory(source(), fieldEvaluator, formatEvaluator);
    }

    private static DateFormatter toFormatter(Object format) {
        return forPattern(((BytesRef) format).utf8ToString());
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new DateParse(
            source(),
            newChildren.get(0),
            newChildren.size() > 1 ? newChildren.get(1) : null,
            newChildren.size() > 2 ? newChildren.get(2) : null
        );
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        Expression format = format();
        Expression field = field();
        Expression options = options();
        Expression first = format != null ? format : field;
        Expression second = format != null ? field : null;
        return NodeInfo.create(this, DateParse::new, first, second, options);
    }
}
