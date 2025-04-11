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
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlConfigurationFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.io.IOException;
import java.time.ZoneId;
import java.time.temporal.ChronoField;
import java.util.List;

import static org.elasticsearch.xpack.esql.expression.EsqlTypeResolutions.isStringAndExact;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.EsqlConverter.STRING_TO_CHRONO_FIELD;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.chronoToLong;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.chronoToLongNanos;

public class DateExtract extends EsqlConfigurationFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "DateExtract",
        DateExtract::new
    );

    private ChronoField chronoField;

    @FunctionInfo(
        returnType = "long",
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
                return new DateExtractConstantNanosEvaluator.Factory(source(), fieldEvaluator, chrono, configuration().zoneId());
            } else {
                return new DateExtractConstantMillisEvaluator.Factory(source(), fieldEvaluator, chrono, configuration().zoneId());
            }
        }

        var chronoEvaluator = toEvaluator.apply(children().get(0));

        if (isNanos) {
            return new DateExtractNanosEvaluator.Factory(source(), fieldEvaluator, chronoEvaluator, configuration().zoneId());
        } else {
            return new DateExtractMillisEvaluator.Factory(source(), fieldEvaluator, chronoEvaluator, configuration().zoneId());
        }

    }

    private ChronoField chronoField(FoldContext ctx) {
        // chronoField’s never checked (the return is). The foldability test is done twice and type is checked in resolveType() already.
        // TODO: move the slimmed down code here to toEvaluator?
        if (chronoField == null) {
            Expression field = children().get(0);
            try {
                if (field.foldable() && DataType.isString(field.dataType())) {
                    chronoField = (ChronoField) STRING_TO_CHRONO_FIELD.convert(field.fold(ctx));
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
}
