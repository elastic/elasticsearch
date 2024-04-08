/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.InvalidArgumentException;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.time.ZoneId;
import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.common.time.DateFormatter.forPattern;
import static org.elasticsearch.xpack.esql.expression.EsqlTypeResolutions.isStringAndExact;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.DEFAULT_DATE_TIME_FORMATTER;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateTimeToLong;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isString;
import static org.elasticsearch.xpack.ql.util.DateUtils.UTC;

public class DateParse extends EsqlScalarFunction implements OptionalArgument {

    private final Expression field;
    private final Expression format;

    @FunctionInfo(returnType = "date", description = "Parses a string into a date value")
    public DateParse(
        Source source,
        @Param(name = "datePattern", type = { "keyword", "text" }, description = "A valid date pattern", optional = true) Expression first,
        @Param(name = "dateString", type = { "keyword", "text" }, description = "A string representing a date") Expression second
    ) {
        super(source, second != null ? List.of(first, second) : List.of(first));
        this.field = second != null ? second : first;
        this.format = second != null ? first : null;
    }

    @Override
    public DataType dataType() {
        return DataTypes.DATETIME;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = isString(field, sourceText(), format != null ? SECOND : FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }
        if (format != null) {
            resolution = isStringAndExact(format, sourceText(), FIRST);
            if (resolution.unresolved()) {
                return resolution;
            }
        }

        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public boolean foldable() {
        return field.foldable() && (format == null || format.foldable());
    }

    @Evaluator(extraName = "Constant", warnExceptions = { IllegalArgumentException.class })
    public static long process(BytesRef val, @Fixed DateFormatter formatter) throws IllegalArgumentException {
        return dateTimeToLong(val.utf8ToString(), formatter);
    }

    @Evaluator(warnExceptions = { IllegalArgumentException.class })
    static long process(BytesRef val, BytesRef formatter, @Fixed ZoneId zoneId) throws IllegalArgumentException {
        return dateTimeToLong(val.utf8ToString(), toFormatter(formatter, zoneId));
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(Function<Expression, ExpressionEvaluator.Factory> toEvaluator) {
        ZoneId zone = UTC; // TODO session timezone?
        ExpressionEvaluator.Factory fieldEvaluator = toEvaluator.apply(field);
        if (format == null) {
            return new DateParseConstantEvaluator.Factory(source(), fieldEvaluator, DEFAULT_DATE_TIME_FORMATTER);
        }
        if (EsqlDataTypes.isString(format.dataType()) == false) {
            throw new IllegalArgumentException("unsupported data type for date_parse [" + format.dataType() + "]");
        }
        if (format.foldable()) {
            try {
                DateFormatter formatter = toFormatter(format.fold(), zone);
                return new DateParseConstantEvaluator.Factory(source(), fieldEvaluator, formatter);
            } catch (IllegalArgumentException e) {
                throw new InvalidArgumentException(e, "invalid date pattern for [{}]: {}", sourceText(), e.getMessage());
            }
        }
        ExpressionEvaluator.Factory formatEvaluator = toEvaluator.apply(format);
        return new DateParseEvaluator.Factory(source(), fieldEvaluator, formatEvaluator, zone);
    }

    private static DateFormatter toFormatter(Object format, ZoneId zone) {
        return forPattern(((BytesRef) format).utf8ToString()).withZone(zone);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new DateParse(source(), newChildren.get(0), newChildren.size() > 1 ? newChildren.get(1) : null);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        Expression first = format != null ? format : field;
        Expression second = format != null ? field : null;
        return NodeInfo.create(this, DateParse::new, first, second);
    }
}
