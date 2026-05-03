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
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.common.util.LocaleUtils;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.EntryExpression;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.MapParam;
import org.elasticsearch.xpack.esql.expression.function.Options;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.TwoOptionalArguments;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlConfigurationFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.io.IOException;
import java.time.DateTimeException;
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

public class DateParse extends EsqlConfigurationFunction implements TwoOptionalArguments {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "DateParse",
        DateParse::new
    );
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(DateParse.class)
        .ternaryConfig(DateParse::new)
        .name("date_parse");

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
                    description = "Coordinated Universal Time (UTC) offset or IANA time zone used to convert date values "
                        + "in the query string to UTC. Can be a literal string (e.g. \"US/Eastern\") or a field reference "
                        + "containing the timezone per document."
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
        ) Expression third,
        Configuration configuration
    ) {
        super(source, fields(first, second, third), configuration);
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
            in.readOptionalNamedWriteable(Expression.class),
            ((PlanStreamInput) in).configuration()
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
    public static long process(BytesRef val, @Fixed DateFormatter formatter, @Fixed ZoneId zoneId, @Fixed Locale locale)
        throws IllegalArgumentException {
        return parse(val.utf8ToString(), formatter, zoneId, locale);
    }

    @Evaluator(warnExceptions = { IllegalArgumentException.class })
    static long process(BytesRef val, BytesRef formatter, @Fixed ZoneId zoneId, @Fixed Locale locale) throws IllegalArgumentException {
        return parse(val.utf8ToString(), toFormatter(formatter, locale), zoneId, locale);
    }

    @Evaluator(extraName = "ConstantRuntimeZoneId", warnExceptions = { IllegalArgumentException.class, ZoneRulesException.class, NullPointerException.class })
    static long process(BytesRef val, @Fixed DateFormatter formatter, BytesRef zoneId, @Fixed Locale locale)
        throws IllegalArgumentException, ZoneRulesException {
        return parse(val.utf8ToString(), formatter, parseZoneId(zoneId), locale);
    }

    @Evaluator(extraName = "RuntimeZoneId", warnExceptions = { IllegalArgumentException.class, ZoneRulesException.class, NullPointerException.class })
    static long process(BytesRef val, BytesRef formatter, BytesRef zoneId, @Fixed Locale locale) throws IllegalArgumentException,
        ZoneRulesException {
        return parse(val.utf8ToString(), toFormatter(formatter, locale), parseZoneId(zoneId), locale);
    }

    private static long parse(String date, DateFormatter formatter, ZoneId zoneId, Locale locale) {
        try {
            return DateFormatters.from(formatter.parse(date), locale, zoneId).toInstant().toEpochMilli();
        } catch (DateTimeException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    private static ZoneId parseZoneId(BytesRef zoneIdStr) {
        if (zoneIdStr == null || zoneIdStr.length == 0) {
            return null;
        }
        return ZoneId.of(zoneIdStr.utf8ToString());
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
        ExpressionEvaluator.Factory fieldEvaluator = toEvaluator.apply(field());

        Expression options = options();

        Locale locale = extractLocale(options);
        Expression timezoneExpr = extractTimezoneExpr(options);

        ZoneId constantTimezone = resolveConstantTimezone(timezoneExpr, toEvaluator);
        ExpressionEvaluator.Factory timezoneEvaluator = resolveTimezoneEvaluator(timezoneExpr, toEvaluator);

        return resolveFormatterAndBuildEvaluator(
            format(),
            locale,
            toEvaluator,
            fieldEvaluator,
            constantTimezone,
            timezoneEvaluator
        );
    }

    private Locale extractLocale(Expression options) {
        if (options == null) {
            return Locale.ROOT;
        }

        for (EntryExpression entry : ((MapExpression) options).entryExpressions()) {
            if (!(entry.key() instanceof Literal keyLit)) {
                throw new InvalidArgumentException("Option keys must be literal values");
            }

            if (LOCALE_PARAM_NAME.equals(BytesRefs.toString(keyLit.value()))) {
                if (!(entry.value() instanceof Literal valLit)) {
                    throw new InvalidArgumentException(
                        "Invalid option [" + LOCALE_PARAM_NAME + "] in [" + sourceText() + "], expected a [keyword] value");
                }
                return LocaleUtils.parse(BytesRefs.toString(valLit.value()));
            }
        }

        return Locale.ROOT;
    }

    private Expression extractTimezoneExpr(Expression options) {
        if (options == null) {
            return null;
        }

        for (EntryExpression entry : ((MapExpression) options).entryExpressions()) {
            if (!(entry.key() instanceof Literal keyLit)) {
                throw new InvalidArgumentException("Option keys must be literal values");
            }

            String optionKey = BytesRefs.toString(keyLit.value());

            if (!ALLOWED_OPTIONS.containsKey(optionKey)) {
                throw new InvalidArgumentException(
                    "Invalid option [" + optionKey + "] in [" + sourceText() + "], expected one of " + ALLOWED_OPTIONS.keySet());
            }

            if (TIME_ZONE_PARAM_NAME.equals(optionKey)) {
                if (!DataType.isString(entry.value().dataType())) {
                    throw new InvalidArgumentException(
                        "Invalid option [" + optionKey + "] in [" + sourceText() + "], expected a [keyword] value");
                }
                return entry.value();
            }
        }

        return null;
    }

    private ZoneId resolveConstantTimezone(Expression timezoneExpr, ToEvaluator toEvaluator) {
        if (timezoneExpr == null) {
            return configuration().zoneId();
        }

        if (!timezoneExpr.foldable()) {
            return null;
        }

        try {
            String tzString = BytesRefs.toString((BytesRef) timezoneExpr.fold(toEvaluator.foldCtx()));
            return ZoneId.of(tzString);
        } catch (ZoneRulesException e) {
            throw new IllegalArgumentException("unsupported timezone [" + timezoneExpr + "]: " + e.getMessage());
        }
    }

    private ExpressionEvaluator.Factory resolveTimezoneEvaluator(Expression timezoneExpr, ToEvaluator toEvaluator) {
        if (timezoneExpr == null || timezoneExpr.foldable()) {
            return null;
        }
        return toEvaluator.apply(timezoneExpr);
    }

    private ExpressionEvaluator.Factory resolveFormatterAndBuildEvaluator(
        Expression format,
        Locale locale,
        ToEvaluator toEvaluator,
        ExpressionEvaluator.Factory fieldEvaluator,
        ZoneId constantTimezone,
        ExpressionEvaluator.Factory timezoneEvaluator) {

        if (format == null) {
            DateFormatter formatter = DEFAULT_DATE_TIME_FORMATTER.withLocale(locale);
            return selectEvaluator(fieldEvaluator, formatter, null, constantTimezone, timezoneEvaluator, locale);
        }

        if (!DataType.isString(format.dataType())) {
            throw new IllegalArgumentException("unsupported data type for date_parse [" + format.dataType() + "]");
        }

        if (format.foldable()) {
            try {
                DateFormatter formatter = toFormatter(format.fold(toEvaluator.foldCtx()), locale);
                return selectEvaluator(fieldEvaluator, formatter, null, constantTimezone, timezoneEvaluator, locale);
            } catch (IllegalArgumentException e) {
                throw new InvalidArgumentException(e, "invalid date pattern for [{}]: {}", sourceText(), e.getMessage());
            }
        }

        // Format is runtime
        ExpressionEvaluator.Factory formatEvaluator = toEvaluator.apply(format);
        return selectEvaluator(fieldEvaluator, null, formatEvaluator, constantTimezone, timezoneEvaluator, locale);
    }

    private ExpressionEvaluator.Factory selectEvaluator(
        ExpressionEvaluator.Factory fieldEvaluator,
        DateFormatter formatter,
        ExpressionEvaluator.Factory formatEvaluator,
        ZoneId constantTimezone,
        ExpressionEvaluator.Factory timezoneEvaluator,
        Locale locale) {

        if (formatEvaluator != null && timezoneEvaluator != null) {
            return new DateParseRuntimeZoneIdEvaluator.Factory(
                source(), fieldEvaluator, formatEvaluator, timezoneEvaluator, locale);
        }

        if (formatEvaluator != null) {
            return new DateParseEvaluator.Factory(
                source(), fieldEvaluator, formatEvaluator, constantTimezone, locale);
        }

        if (timezoneEvaluator != null) {
            return new DateParseConstantRuntimeZoneIdEvaluator.Factory(
                source(), fieldEvaluator, formatter, timezoneEvaluator, locale);
        }

        return new DateParseConstantEvaluator.Factory(
            source(), fieldEvaluator, formatter, constantTimezone, locale);
    }

    private static DateFormatter toFormatter(Object format, Locale locale) {
        return forPattern(((BytesRef) format).utf8ToString()).withLocale(locale);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new DateParse(
            source(),
            newChildren.get(0),
            newChildren.size() > 1 ? newChildren.get(1) : null,
            newChildren.size() > 2 ? newChildren.get(2) : null,
            configuration()
        );
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        Expression format = format();
        Expression field = field();
        Expression options = options();
        Expression first = format != null ? format : field;
        Expression second = format != null ? field : null;
        return NodeInfo.create(this, DateParse::new, first, second, options, configuration());
    }
}
