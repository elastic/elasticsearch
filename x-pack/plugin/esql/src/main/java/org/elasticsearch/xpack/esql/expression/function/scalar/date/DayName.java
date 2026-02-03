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
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
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
import java.time.format.TextStyle;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;

public class DayName extends EsqlConfigurationFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "DayName", DayName::new);

    private final Expression field;

    @FunctionInfo(
        returnType = "keyword",
        description = "Returns the name of the weekday for date based on the configured Locale.",
        examples = @Example(file = "date", tag = "docsDayName"),
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA, version = "9.2.0") }
    )
    public DayName(
        Source source,
        @Param(
            name = "date",
            type = { "date", "date_nanos" },
            description = "Date expression. If `null`, the function returns `null`."
        ) Expression date,
        Configuration configuration
    ) {
        super(source, List.of(date), configuration);
        this.field = date;
    }

    private DayName(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), ((PlanStreamInput) in).configuration());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    Expression field() {
        return field;
    }

    @Override
    public DataType dataType() {
        return DataType.KEYWORD;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        String operationName = sourceText();
        TypeResolution resolution = TypeResolutions.isType(field, DataType::isDate, operationName, FIRST, "datetime or date_nanos");
        if (resolution.unresolved()) {
            return resolution;
        }

        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public boolean foldable() {
        return field.foldable();
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        var fieldEvaluator = toEvaluator.apply(field);
        if (field().dataType() == DataType.DATE_NANOS) {
            return new DayNameNanosEvaluator.Factory(source(), fieldEvaluator, configuration().zoneId(), configuration().locale());
        }
        return new DayNameMillisEvaluator.Factory(source(), fieldEvaluator, configuration().zoneId(), configuration().locale());
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new DayName(source(), newChildren.get(0), configuration());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, DayName::new, field, configuration());
    }

    @Evaluator(extraName = "Millis")
    static BytesRef processMillis(long val, @Fixed ZoneId zoneId, @Fixed Locale locale) {
        ZonedDateTime dateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(val), zoneId);
        String displayName = dateTime.getDayOfWeek().getDisplayName(TextStyle.FULL, locale);
        return new BytesRef(displayName);
    }

    @Evaluator(extraName = "Nanos")
    static BytesRef processNanos(long val, @Fixed ZoneId zoneId, @Fixed Locale locale) {
        ZonedDateTime dateTime = ZonedDateTime.ofInstant(Instant.ofEpochSecond(0L, val), zoneId);
        String displayName = dateTime.getDayOfWeek().getDisplayName(TextStyle.FULL, locale);
        return new BytesRef(displayName);
    }
}
