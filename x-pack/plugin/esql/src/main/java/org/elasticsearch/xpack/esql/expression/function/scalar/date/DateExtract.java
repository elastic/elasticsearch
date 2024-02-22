/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlConfigurationFunction;
import org.elasticsearch.xpack.ql.InvalidArgumentException;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.TypeResolutions;
import org.elasticsearch.xpack.ql.session.Configuration;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoField;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isDate;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isStringAndExact;

public class DateExtract extends EsqlConfigurationFunction {

    private ChronoField chronoField;

    @FunctionInfo(returnType = "long", description = "Extracts parts of a date, like year, month, day, hour.")
    public DateExtract(
        Source source,
        // Need to replace the commas in the description here with semi-colon as there's a bug in the CSV parser
        // used in the CSVTests and fixing it is not trivial
        @Param(name = "date_part", type = { "keyword" }, description = """
            Part of the date to extract.
            Can be: aligned_day_of_week_in_month; aligned_day_of_week_in_year; aligned_week_of_month;
            aligned_week_of_year; ampm_of_day; clock_hour_of_ampm; clock_hour_of_day; day_of_month; day_of_week;
            day_of_year; epoch_day; era; hour_of_ampm; hour_of_day; instant_seconds; micro_of_day; micro_of_second;
            milli_of_day; milli_of_second; minute_of_day; minute_of_hour; month_of_year; nano_of_day; nano_of_second;
            offset_seconds; proleptic_month; second_of_day; second_of_minute; year; or year_of_era.""") Expression chronoFieldExp,
        @Param(name = "field", type = "date", description = "Date expression") Expression field,
        Configuration configuration
    ) {
        super(source, List.of(chronoFieldExp, field), configuration);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(Function<Expression, ExpressionEvaluator.Factory> toEvaluator) {
        var fieldEvaluator = toEvaluator.apply(children().get(1));
        if (children().get(0).foldable()) {
            ChronoField chrono = chronoField();
            if (chrono == null) {
                BytesRef field = (BytesRef) children().get(0).fold();
                throw new InvalidArgumentException("invalid date field for [{}]: {}", sourceText(), field.utf8ToString());
            }
            return new DateExtractConstantEvaluator.Factory(source(), fieldEvaluator, chrono, configuration().zoneId());
        }
        var chronoEvaluator = toEvaluator.apply(children().get(0));
        return new DateExtractEvaluator.Factory(source(), fieldEvaluator, chronoEvaluator, configuration().zoneId());
    }

    private ChronoField chronoField() {
        // chronoField's never checked (the return is). The foldability test is done twice and type is checked in resolveType() already.
        // TODO: move the slimmed down code here to toEvaluator?
        if (chronoField == null) {
            Expression field = children().get(0);
            if (field.foldable() && field.dataType() == DataTypes.KEYWORD) {
                try {
                    BytesRef br = BytesRefs.toBytesRef(field.fold());
                    chronoField = ChronoField.valueOf(br.utf8ToString().toUpperCase(Locale.ROOT));
                } catch (Exception e) {
                    return null;
                }
            }
        }
        return chronoField;
    }

    @Evaluator(warnExceptions = { IllegalArgumentException.class })
    static long process(long value, BytesRef chronoField, @Fixed ZoneId zone) {
        ChronoField chrono = ChronoField.valueOf(chronoField.utf8ToString().toUpperCase(Locale.ROOT));
        return Instant.ofEpochMilli(value).atZone(zone).getLong(chrono);
    }

    @Evaluator(extraName = "Constant")
    static long process(long value, @Fixed ChronoField chronoField, @Fixed ZoneId zone) {
        return Instant.ofEpochMilli(value).atZone(zone).getLong(chronoField);
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
        return DataTypes.LONG;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        return isStringAndExact(children().get(0), sourceText(), TypeResolutions.ParamOrdinal.FIRST).and(
            isDate(children().get(1), sourceText(), TypeResolutions.ParamOrdinal.SECOND)
        );
    }

    @Override
    public boolean foldable() {
        return children().get(0).foldable() && children().get(1).foldable();
    }
}
