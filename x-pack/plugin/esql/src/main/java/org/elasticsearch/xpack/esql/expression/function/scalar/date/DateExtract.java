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
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.AnyNullIsNull;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.Signature;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlConfigurationFunction;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.esql.expression.EsqlTypeResolutions.isStringAndExact;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.chronoToLong;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.chronoToLongNanos;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.stringToChrono;

public class DateExtract extends EsqlConfigurationFunction implements AnyNullIsNull {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "DateExtract",
        DateExtract::new
    );
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(DateExtract.class)
        .binaryConfig(DateExtract::new)
        .name("date_extract");

    /**
     * Chrono fields whose extracted value is a single half-open interval on the timeline
     * in the query time zone. {@code YEAR_OF_ERA} is excluded: it repeats across BCE/CE.
     */
    private static final Set<ChronoField> MONOTONIC_CHRONOS = Set.of(ChronoField.YEAR, ChronoField.PROLEPTIC_MONTH, ChronoField.EPOCH_DAY);

    private ChronoField chronoField;

    @FunctionInfo(
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA) },
        returnType = "long",
        signatures = { @Signature(params = { "STRING", "date|date_nanos" }, returnType = "long") },
        briefSummary = "Extracts parts of a date, like year, month, day, hour.",
        description = "Extracts parts of a date, like year, month, day, hour.",
        examples = {
            @Example(file = "date", tag = "dateExtract"),
            @Example(
                file = "date",
                tag = "docsDateExtractBusinessHours",
                description = "Find all events that occurred outside of business hours (before 9 AM or after 5PM), on any given date:"
            ) }
    )
    public DateExtract(
        Source source,
        // Need to replace the commas in the description here with semi-colon as there’s a bug in the CSV parser
        // used in the CSVTests and fixing it is not trivial
        @Param(name = "datePart", type = { "keyword", "text" }, description = """
            Part of the date to extract.\n
            Can be: `aligned_day_of_week_in_month`, `aligned_day_of_week_in_year`, `aligned_week_of_month`, `aligned_week_of_year`,
            `ampm_of_day`, `clock_hour_of_ampm`, `clock_hour_of_day`, `day_of_month`, `day_of_week`, `day_of_year`, `epoch_day`,
            `era`, `hour_of_ampm`, `hour_of_day`, `instant_seconds`, `micro_of_day`, `micro_of_second`, `milli_of_day`,
            `milli_of_second`, `minute_of_day`, `minute_of_hour`, `month_of_year`, `nano_of_day`, `nano_of_second`,
            `offset_seconds`, `proleptic_month`, `second_of_day`, `second_of_minute`, `year`, or `year_of_era`.
            Refer to {javadoc8}/java/time/temporal/ChronoField.html[java.time.temporal.ChronoField]
            for a description of these values.\n
            If `null`, the function returns `null`.""") Expression chronoFieldExp,
        @Param(
            name = "date",
            type = { "date", "date_nanos" },
            description = "Date expression. If `null`, the function returns `null`."
        ) Expression field,
        Configuration configuration
    ) {
        super(source, List.of(chronoFieldExp, field), configuration);
    }

    private DateExtract(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            ((PlanStreamInput) in).configuration()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(datePart());
        out.writeNamedWriteable(field());
    }

    Expression datePart() {
        return children().get(0);
    }

    Expression field() {
        return children().get(1);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        boolean isNanos = switch (field().dataType()) {
            case DATETIME -> false;
            case DATE_NANOS -> true;
            default -> throw new UnsupportedOperationException(
                "Unsupported field type ["
                    + field().dataType().name()
                    + "]. "
                    + "If you're seeing this, there’s a bug in DateExtract.resolveType"
            );
        };

        ExpressionEvaluator.Factory fieldEvaluator = toEvaluator.apply(children().get(1));

        // Constant chrono field
        if (children().get(0).foldable()) {
            ChronoField chrono = chronoField(toEvaluator.foldCtx());
            if (chrono == null) {
                BytesRef field = (BytesRef) children().get(0).fold(toEvaluator.foldCtx());
                throw new InvalidArgumentException("invalid date field for [{}]: {}", sourceText(), field.utf8ToString());
            }

            if (isNanos) {
                return new DateExtractConstantNanosEvaluator.Factory(
                    source(),
                    fieldEvaluator,
                    chrono,
                    QuerySettings.TIME_ZONE.get(configuration().resolvedSettings())
                );
            } else {
                return new DateExtractConstantMillisEvaluator.Factory(
                    source(),
                    fieldEvaluator,
                    chrono,
                    QuerySettings.TIME_ZONE.get(configuration().resolvedSettings())
                );
            }
        }

        var chronoEvaluator = toEvaluator.apply(children().get(0));

        if (isNanos) {
            return new DateExtractNanosEvaluator.Factory(
                source(),
                fieldEvaluator,
                chronoEvaluator,
                QuerySettings.TIME_ZONE.get(configuration().resolvedSettings())
            );
        } else {
            return new DateExtractMillisEvaluator.Factory(
                source(),
                fieldEvaluator,
                chronoEvaluator,
                QuerySettings.TIME_ZONE.get(configuration().resolvedSettings())
            );
        }

    }

    private ChronoField chronoField(FoldContext ctx) {
        // chronoField’s never checked (the return is). The foldability test is done twice and type is checked in resolveType() already.
        // TODO: move the slimmed down code here to toEvaluator?
        if (chronoField == null) {
            Expression field = children().get(0);
            try {
                if (field.foldable() && DataType.isString(field.dataType())) {
                    var foldedValue = field.fold(ctx);
                    if (foldedValue != null) {
                        chronoField = stringToChrono(foldedValue);
                    }
                }
            } catch (Exception e) {
                return null;
            }
        }
        return chronoField;
    }

    @Evaluator(extraName = "Millis", warnExceptions = { IllegalArgumentException.class })
    static long processMillis(long value, BytesRef chronoField, @Fixed ZoneId zone) {
        return chronoToLong(value, chronoField, zone);
    }

    @Evaluator(extraName = "ConstantMillis")
    static long processMillis(long value, @Fixed ChronoField chronoField, @Fixed ZoneId zone) {
        return chronoToLong(value, chronoField, zone);
    }

    @Evaluator(extraName = "Nanos", warnExceptions = { IllegalArgumentException.class })
    static long processNanos(long value, BytesRef chronoField, @Fixed ZoneId zone) {
        return chronoToLongNanos(value, chronoField, zone);
    }

    @Evaluator(extraName = "ConstantNanos")
    static long processNanos(long value, @Fixed ChronoField chronoField, @Fixed ZoneId zone) {
        return chronoToLongNanos(value, chronoField, zone);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new DateExtract(source(), newChildren.get(0), newChildren.get(1), configuration());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, DateExtract::new, children().get(0), children().get(1), configuration());
    }

    @Override
    public DataType dataType() {
        return DataType.LONG;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        String operationName = sourceText();
        return isStringAndExact(children().get(0), sourceText(), TypeResolutions.ParamOrdinal.FIRST).and(
            TypeResolutions.isType(
                children().get(1),
                DataType::isDate,
                operationName,
                TypeResolutions.ParamOrdinal.SECOND,
                "datetime or date_nanos"
            )
        );
    }

    @Override
    public boolean foldable() {
        return children().get(0).foldable() && children().get(1).foldable();
    }

    /**
     * Fold an all-literal call without constructing a {@code DateExtract} node. Listing runs
     * before ImplicitCasting, so KEYWORD ISO datetimes are parsed here. Returns {@code null}
     * when the call cannot be folded; does not call {@code dataType()} on unresolved children.
     * {@code source} is the unresolved call being replaced.
     */
    static Literal tryFoldLiterals(Source source, List<Expression> args, Configuration configuration) {
        Literal[] literals = DateFunctionLiterals.literalArgs(args, 2);
        if (literals == null) {
            return null;
        }
        Literal datePart = literals[0];
        Literal field = literals[1];
        if (datePart.value() == null || field.value() == null) {
            return null;
        }
        if (DataType.isString(datePart.dataType()) == false) {
            return null;
        }
        ChronoField chrono = stringToChrono(datePart.value());
        if (chrono == null) {
            return null;
        }
        ZoneId zone = DateFunctionLiterals.zoneId(configuration);
        DateFunctionLiterals.ParsedDate parsed = DateFunctionLiterals.parseDateLiteral(field, zone);
        if (parsed == null) {
            return null;
        }
        long extracted = parsed.nanos() ? processNanos(parsed.epoch(), chrono, zone) : processMillis(parsed.epoch(), chrono, zone);
        return new Literal(source, extracted, DataType.LONG);
    }

    /**
     * Invert {@code DATE_EXTRACT(chrono, field) op literal} for monotonic chronos only.
     * Cyclic extracts stay as function comparisons. Returns {@code null} to leave the
     * comparison unchanged.
     */
    static Expression tryRewriteComparison(DateExtract extract, EsqlBinaryComparison cmp, FoldContext ctx) {
        Expression field = DateFunctionLiterals.datetimeField(extract.field());
        if (field == null) {
            return null;
        }
        if (extract.datePart().foldable() == false) {
            return null;
        }
        Object chronoValue = extract.datePart().fold(ctx);
        if (chronoValue == null) {
            return null;
        }
        ChronoField chrono = stringToChrono(chronoValue);
        if (chrono == null || MONOTONIC_CHRONOS.contains(chrono) == false) {
            return null;
        }
        Long extracted = DateFunctionLiterals.foldIntegralNumber(cmp.right(), ctx);
        if (extracted == null) {
            return null;
        }
        ZoneId zone = DateFunctionLiterals.zoneId(extract.configuration());
        long[] bounds = extractBucketBounds(chrono, extracted, zone, field.dataType());
        return DateFunctionComparisonRewriter.rewriteComparisonBounds(
            cmp,
            field,
            DateFunctionComparisonRewriter.boundLiteral(cmp, bounds[0], field.dataType()),
            DateFunctionComparisonRewriter.boundLiteral(cmp, bounds[1], field.dataType()),
            true
        );
    }

    private static long[] extractBucketBounds(ChronoField chrono, long value, ZoneId zone, DataType fieldType) {
        ZonedDateTime start = switch (chrono) {
            case YEAR -> LocalDate.of(Math.toIntExact(value), 1, 1).atStartOfDay(zone);
            case PROLEPTIC_MONTH -> {
                long year = Math.floorDiv(value, 12);
                int month = Math.toIntExact(Math.floorMod(value, 12L)) + 1;
                yield LocalDate.of(Math.toIntExact(year), month, 1).atStartOfDay(zone);
            }
            case EPOCH_DAY -> LocalDate.ofEpochDay(value).atStartOfDay(zone);
            default -> throw new IllegalArgumentException("unexpected chrono [" + chrono + "]");
        };
        ZonedDateTime next = switch (chrono) {
            case YEAR -> start.plusYears(1);
            case PROLEPTIC_MONTH -> start.plusMonths(1);
            case EPOCH_DAY -> start.plusDays(1);
            default -> throw new IllegalArgumentException("unexpected chrono [" + chrono + "]");
        };
        return new long[] {
            DateFunctionLiterals.toFieldEpoch(start.toInstant(), fieldType),
            DateFunctionLiterals.toFieldEpoch(next.toInstant(), fieldType) };
    }
}
