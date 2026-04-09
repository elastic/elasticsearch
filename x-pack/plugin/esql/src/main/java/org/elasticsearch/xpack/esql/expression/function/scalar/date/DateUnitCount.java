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
import java.time.temporal.ChronoUnit;
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
 * Counts how many {@code dstUnit} values fit into one {@code srcUnit} interval for the given date.
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

    private final Expression dstUnit;
    private final Expression srcUnit;
    private final Expression date;

    @FunctionInfo(
        returnType = "long",
        description = "Counts how many `dst_unit` values are contained in a single `src_unit` period for `date`.",
        examples = @Example(
            description = "Count the number of days in February for a timestamp using explicit units.",
            file = "date",
            tag = "docsDateUnitCount"
        )
    )
    public DateUnitCount(
        Source source,
        @Param(name = "dst_unit", type = { "keyword" }, description = "The unit to count.") Expression dstUnit,
        @Param(name = "src_unit", type = { "keyword" }, description = "The enclosing container unit.") Expression srcUnit,
        @Param(name = "date", type = { "date", "date_nanos" }, description = "Date expression.") Expression date,
        Configuration configuration
    ) {
        super(source, List.of(dstUnit, srcUnit, date), configuration);
        this.dstUnit = dstUnit;
        this.srcUnit = srcUnit;
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
        out.writeNamedWriteable(dstUnit);
        out.writeNamedWriteable(srcUnit);
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
        return dstUnit.foldable() && srcUnit.foldable() && date.foldable();
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        var operation = sourceText();
        return isStringAndExact(dstUnit, operation, FIRST).and(isStringAndExact(srcUnit, operation, SECOND))
            .and(TypeResolutions.isType(date, DataType::isDate, operation, THIRD, "datetime or date_nanos"));
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        var dateEvaluator = toEvaluator.apply(date);
        var zoneId = configuration().zoneId();

        DateDiff.Part dst = null;
        if (dstUnit.foldable() != false) {
            dst = parsePart(dstUnit.fold(toEvaluator.foldCtx()));
        }

        DateDiff.Part src = null;
        if (srcUnit.foldable() != false) {
            src = parsePart(srcUnit.fold(toEvaluator.foldCtx()));
        }

        if (dst != null && src != null) {
            return date.dataType() == DataType.DATE_NANOS
                ? new DateUnitCountConstantNanosEvaluator.Factory(source(), dst, src, dateEvaluator, zoneId)
                : new DateUnitCountConstantMillisEvaluator.Factory(source(), dst, src, dateEvaluator, zoneId);
        }

        return date.dataType() == DataType.DATE_NANOS
            ? new DateUnitCountNanosEvaluator.Factory(
                source(),
                toEvaluator.apply(dstUnit),
                toEvaluator.apply(srcUnit),
                dateEvaluator,
                zoneId
            )
            : new DateUnitCountMillisEvaluator.Factory(
                source(),
                toEvaluator.apply(dstUnit),
                toEvaluator.apply(srcUnit),
                dateEvaluator,
                zoneId
            );
    }

    private static DateDiff.Part parsePart(Object value) {
        if (value == null) {
            return null;
        }

        String text;
        if (value instanceof BytesRef b) {
            text = b.utf8ToString();
        } else {
            text = value.toString();
        }

        return DateDiff.Part.resolve(text);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new DateUnitCount(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2), configuration());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, DateUnitCount::new, dstUnit, srcUnit, date, configuration());
    }

    @Evaluator(extraName = "ConstantMillis")
    static long processMillis(@Fixed DateDiff.Part dstUnit, @Fixed DateDiff.Part scrUnit, long date, @Fixed ZoneId zoneId) {
        return count(dstUnit, scrUnit, ofInstant(ofEpochMilli(date), zoneId));
    }

    @Evaluator(extraName = "ConstantNanos")
    static long processNanos(@Fixed DateDiff.Part dstUnit, @Fixed DateDiff.Part srcUnit, long date, @Fixed ZoneId zoneId) {
        return count(dstUnit, srcUnit, ofInstant(ofEpochSecond(0L, date), zoneId));
    }

    @Evaluator(extraName = "Millis")
    static long processMillis(BytesRef dstUnit, BytesRef srcUnit, long date, @Fixed ZoneId zoneId) {
        return processMillis(parsePart(dstUnit), parsePart(srcUnit), date, zoneId);
    }

    @Evaluator(extraName = "Nanos")
    static long processNanos(BytesRef dstUnit, BytesRef srcUnit, long date, @Fixed ZoneId zoneId) {
        return processNanos(parsePart(dstUnit), parsePart(srcUnit), date, zoneId);
    }

    private static long count(@Fixed DateDiff.Part dstUnit, @Fixed DateDiff.Part srcUnit, ZonedDateTime timestamp) {
        return switch (srcUnit) {
            case HOUR -> {
                var start = timestamp.truncatedTo(ChronoUnit.HOURS);
                yield dstUnit.diff(start, start.plusHours(1));
            }
            case DAY -> {
                var start = timestamp.toLocalDate().atStartOfDay(timestamp.getZone());
                yield dstUnit.diff(start, start.plusDays(1));
            }
            case WEEK -> {
                var start = timestamp.with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY))
                    .toLocalDate()
                    .atStartOfDay(timestamp.getZone());
                yield dstUnit.diff(start, start.plusWeeks(1));
            }
            case MONTH -> {
                var start = timestamp.with(TemporalAdjusters.firstDayOfMonth()).toLocalDate().atStartOfDay(timestamp.getZone());
                yield dstUnit.diff(start, start.plusMonths(1));
            }
            case YEAR -> {
                var start = timestamp.with(TemporalAdjusters.firstDayOfYear()).toLocalDate().atStartOfDay(timestamp.getZone());
                yield dstUnit.diff(start, start.plusYears(1));
            }
            default -> throw new IllegalArgumentException("Unsupported unit [" + srcUnit + "]");
        };
    }

}
