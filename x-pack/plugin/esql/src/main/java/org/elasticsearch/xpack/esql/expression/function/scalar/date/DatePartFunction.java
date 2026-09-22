/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.AnyNullIsNull;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.OnlySurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlConfigurationFunction;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;

/**
 * Unary sugar over {@link DateExtract} with a fixed chrono field.
 * <p>
 * Public {@code YEAR}/{@code MONTH}/{@code DAY}/{@code HOUR} cannot be registry
 * aliases of {@code DATE_EXTRACT}: that class is binary. After analysis,
 * {@link #surrogate()} is {@code DATE_EXTRACT} so monotonic invert of
 * {@code YEAR(ts)} is free. Listing fold runs before analysis and keys on
 * the registered class, so {@link DateFunctionComparisonRewriter} must
 * dispatch here and inject the chrono.
 */
abstract class DatePartFunction extends EsqlConfigurationFunction implements OnlySurrogateExpression, AnyNullIsNull {

    private final Expression field;
    private final String chronoField;

    DatePartFunction(Source source, Expression field, Configuration configuration, String chronoField) {
        super(source, List.of(field), configuration);
        this.field = field;
        this.chronoField = chronoField;
    }

    Expression field() {
        return field;
    }

    String chronoField() {
        return chronoField;
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
        return TypeResolutions.isType(field, DataType::isDate, sourceText(), FIRST, "datetime or date_nanos");
    }

    @Override
    public boolean foldable() {
        return field.foldable();
    }

    @Override
    public Object fold(FoldContext ctx) {
        return surrogate().fold(ctx);
    }

    @Override
    public Expression surrogate() {
        return new DateExtract(source(), Literal.keyword(source(), chronoField), field, configuration());
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        throw new UnsupportedOperationException(getClass().getSimpleName() + " should have been replaced by DateExtract via surrogate()");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException(
            getClass().getSimpleName() + " is a surrogate; lowered to DateExtract before serialization"
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException(
            getClass().getSimpleName() + " is a surrogate; lowered to DateExtract before serialization"
        );
    }

    /**
     * Listing-time fold of an all-literal unary call. Injects this function's
     * chrono and delegates to {@link DateExtract#tryFoldLiterals}.
     */
    static Literal tryFoldLiterals(String chronoField, Source source, List<Expression> args, Configuration configuration) {
        if (args.size() != 1) {
            return null;
        }
        return DateExtract.tryFoldLiterals(source, List.of(Literal.keyword(source, chronoField), args.get(0)), configuration);
    }

    /**
     * Chrono string for a registered date-part class, or {@code null} if
     * {@code clazz} is not one of the unary sugars.
     */
    static String chronoFor(Class<?> clazz) {
        if (clazz == Year.class) {
            return "year";
        }
        if (clazz == Month.class) {
            return "month_of_year";
        }
        if (clazz == Day.class) {
            return "day_of_month";
        }
        if (clazz == Hour.class) {
            return "hour_of_day";
        }
        return null;
    }
}
