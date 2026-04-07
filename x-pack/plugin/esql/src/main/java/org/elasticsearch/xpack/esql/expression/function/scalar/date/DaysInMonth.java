/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

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
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlConfigurationFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;

/**
 * Returns the number of days in the month for a given date
 */
public class DaysInMonth extends EsqlConfigurationFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "DaysInMonth",
        DaysInMonth::new
    );
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(DaysInMonth.class)
        .unaryConfig(DaysInMonth::new)
        .name("days_in_month");

    private final Expression date;

    @FunctionInfo(
        returnType = "long",
        description = "Returns the number of days in the month for a given date.",
        examples = @Example(file = "date", tag = "docsDaysInMonth"),
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA, version = "9.4.0") }
    )
    public DaysInMonth(
        Source source,
        @Param(
            name = "date",
            type = { "date", "date_nanos" },
            description = "Date expression. If `null`, the function returns `null`."
        ) Expression date,
        Configuration configuration
    ) {
        super(source, List.of(date), configuration);
        this.date = date;
    }

    private DaysInMonth(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), ((PlanStreamInput) in).configuration());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(date);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    Expression field() {
        return date;
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
        return TypeResolutions.isType(date, DataType::isDate, operationName, FIRST, "datetime or date_nanos");
    }

    @Override
    public boolean foldable() {
        return date.foldable();
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        var dateEvaluator = toEvaluator.apply(date);
        if (date.dataType() == DataType.DATE_NANOS) {
            return new DaysInMonthNanosEvaluator.Factory(source(), dateEvaluator, configuration().zoneId());
        }
        return new DaysInMonthMillisEvaluator.Factory(source(), dateEvaluator, configuration().zoneId());
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new DaysInMonth(source(), newChildren.getFirst(), configuration());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, DaysInMonth::new, date, configuration());
    }

    @Evaluator(extraName = "Millis")
    static long processMillis(long val, @Fixed ZoneId zoneId) {
        ZonedDateTime dateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(val), zoneId);
        return dateTime.toLocalDate().lengthOfMonth();
    }

    @Evaluator(extraName = "Nanos")
    static long processNanos(long val, @Fixed ZoneId zoneId) {
        ZonedDateTime dateTime = ZonedDateTime.ofInstant(Instant.ofEpochSecond(0L, val), zoneId);
        return dateTime.toLocalDate().lengthOfMonth();
    }
}
