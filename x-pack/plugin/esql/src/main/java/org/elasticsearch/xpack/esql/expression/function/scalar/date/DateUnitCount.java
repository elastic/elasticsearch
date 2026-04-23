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
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlConfigurationFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.io.IOException;
import java.time.DayOfWeek;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAdjusters;
import java.util.List;

import static java.time.Instant.ofEpochMilli;
import static java.time.Instant.ofEpochSecond;
import static java.time.ZonedDateTime.ofInstant;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isStringAndExact;

/**
 * Counts how many {@code toUnit} values fit into one {@code fromUnit} interval for the given date.
 */
public class DateUnitCount extends EsqlConfigurationFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "DateUnitCount",
        DateUnitCount::new
    );

    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(DateUnitCount.class)
        .ternaryConfig(DateUnitCount::new)
        .name("date_unit_count");

    private final Expression toUnit;
    private final Expression fromUnit;
    private final Expression date;

    @FunctionInfo(
        returnType = "long",
        description = "Counts how many `to_unit` values are contained in a single `from_unit` period for `date`.",
        examples = @Example(
            description = "Count the number of days in February for a timestamp using explicit units.",
            file = "date",
            tag = "docsDateUnitCount"
        )
    )
    public DateUnitCount(
        Source source,
        @Param(name = "to_unit", type = { "keyword", "text" }, description = "The unit to count.") Expression toUnit,
        @Param(name = "from_unit", type = { "keyword", "text" }, description = "The enclosing container unit.") Expression fromUnit,
        @Param(name = "date", type = { "date", "date_nanos" }, description = "Date expression.") Expression date,
        Configuration configuration
    ) {
        super(source, List.of(toUnit, fromUnit, date), configuration);
        this.toUnit = toUnit;
        this.fromUnit = fromUnit;
        this.date = date;
    }

    private DateUnitCount(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            ((PlanStreamInput) in).configuration()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(toUnit);
        out.writeNamedWriteable(fromUnit);
        out.writeNamedWriteable(date);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return DataType.LONG;
    }

    @Override
    public boolean foldable() {
        if (toUnit.foldable() && fromUnit.foldable() && date.foldable()) {
            return true;
        }

        DateDiff.Part dst = foldedPart(toUnit, FoldContext.small());
        DateDiff.Part src = foldedPart(fromUnit, FoldContext.small());

        return dst != null && src != null && constCount(dst, src) != null;
    }

    @Override
    public Object fold(FoldContext ctx) {
        DateDiff.Part dst = foldedPart(toUnit, ctx);
        DateDiff.Part src = foldedPart(fromUnit, ctx);

        if (dst != null && src != null) {
            Long constant = constCount(dst, src);
            if (constant != null) {
                return constant;
            }
        }

        return super.fold(ctx);
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        var operation = sourceText();
        return isStringAndExact(toUnit, operation, FIRST).and(isStringAndExact(fromUnit, operation, SECOND))
            .and(TypeResolutions.isType(date, DataType::isDate, operation, THIRD, "datetime or date_nanos"));
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        var dateEvaluator = toEvaluator.apply(date);
        var zoneId = configuration().zoneId();

        DateDiff.Part dst = foldedPart(toUnit, toEvaluator.foldCtx());
        DateDiff.Part src = foldedPart(fromUnit, toEvaluator.foldCtx());

        if (dst != null && src != null) {
            return date.dataType() == DataType.DATE_NANOS
                ? new DateUnitCountConstantNanosEvaluator.Factory(source(), dst, src, dateEvaluator, zoneId)
                : new DateUnitCountConstantMillisEvaluator.Factory(source(), dst, src, dateEvaluator, zoneId);
        }

        return date.dataType() == DataType.DATE_NANOS
            ? new DateUnitCountNanosEvaluator.Factory(
                source(),
                toEvaluator.apply(toUnit),
                toEvaluator.apply(fromUnit),
                dateEvaluator,
                zoneId
            )
            : new DateUnitCountMillisEvaluator.Factory(
                source(),
                toEvaluator.apply(toUnit),
                toEvaluator.apply(fromUnit),
                dateEvaluator,
                zoneId
            );
    }

    private static DateDiff.Part foldedPart(Expression expression, FoldContext ctx) {
        return expression.foldable() ? parsePart(expression.fold(ctx)) : null;
    }

    private static DateDiff.Part parsePart(Object value) {
        if (value == null) {
            return null;
        }
        return DateDiff.Part.resolve(value instanceof BytesRef b ? b.utf8ToString() : value.toString());
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new DateUnitCount(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2), configuration());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, DateUnitCount::new, toUnit, fromUnit, date, configuration());
    }

    @Evaluator(extraName = "ConstantMillis")
    static long processMillis(@Fixed DateDiff.Part toUnit, @Fixed DateDiff.Part fromUnit, long date, @Fixed ZoneId zoneId) {
        return count(toUnit, fromUnit, ofInstant(ofEpochMilli(date), zoneId));
    }

    @Evaluator(extraName = "ConstantNanos")
    static long processNanos(@Fixed DateDiff.Part toUnit, @Fixed DateDiff.Part fromUnit, long date, @Fixed ZoneId zoneId) {
        return count(toUnit, fromUnit, ofInstant(ofEpochSecond(0L, date), zoneId));
    }

    @Evaluator(extraName = "Millis")
    static long processMillis(BytesRef toUnit, BytesRef fromUnit, long date, @Fixed ZoneId zoneId) {
        return processMillis(parsePart(toUnit), parsePart(fromUnit), date, zoneId);
    }

    @Evaluator(extraName = "Nanos")
    static long processNanos(BytesRef toUnit, BytesRef fromUnit, long date, @Fixed ZoneId zoneId) {
        return processNanos(parsePart(toUnit), parsePart(fromUnit), date, zoneId);
    }

    private static long count(DateDiff.Part toUnit, DateDiff.Part fromUnit, ZonedDateTime timestamp) {
        Long constant = constCount(toUnit, fromUnit);
        if (constant != null) {
            return constant;
        }
        // Only variable-length from_units reach here
        final var zone = timestamp.getZone();
        if (fromUnit == DateDiff.Part.DAY) {
            var start = timestamp.toLocalDate().atStartOfDay(zone);
            return toUnit.diff(start, start.plusDays(1));
        } else if (fromUnit == DateDiff.Part.WEEK) {
            var start = timestamp.with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY)).toLocalDate().atStartOfDay(zone);
            return toUnit.diff(start, start.plusWeeks(1));
        } else if (fromUnit == DateDiff.Part.MONTH) {
            var start = timestamp.with(TemporalAdjusters.firstDayOfMonth()).toLocalDate().atStartOfDay(zone);
            return toUnit.diff(start, start.plusMonths(1));
        } else if (fromUnit == DateDiff.Part.YEAR) {
            var start = timestamp.with(TemporalAdjusters.firstDayOfYear()).toLocalDate().atStartOfDay(zone);
            return toUnit.diff(start, start.plusYears(1));
        }
        throw new IllegalArgumentException("Unsupported unit [" + fromUnit + "]");
    }

    static Long constCount(DateDiff.Part toUnit, DateDiff.Part fromUnit) {
        if (toUnit == fromUnit) {
            return 1L;
        }

        Long srcNanos = constNanos(fromUnit);
        Long dstNanos = constNanos(toUnit);
        if (srcNanos != null && dstNanos != null) {
            return srcNanos / dstNanos;
        }

        if (fromUnit == DateDiff.Part.WEEK) {
            return switch (toUnit) {
                case DAY, DAYOFYEAR, WEEKDAY -> 7L;
                default -> null;
            };
        }

        if (fromUnit == DateDiff.Part.YEAR) {
            return switch (toUnit) {
                case MONTH -> 12L;
                case QUARTER -> 4L;
                default -> null;
            };
        }

        return null;
    }

    private static Long constNanos(DateDiff.Part unit) {
        return switch (unit) {
            case NANOSECOND -> 1L;
            case MICROSECOND -> 1_000L;
            case MILLISECOND -> 1_000_000L;
            case SECOND -> 1_000_000_000L;
            case MINUTE -> 60_000_000_000L;
            case HOUR -> 3_600_000_000_000L;
            default -> null;
        };
    }
}
