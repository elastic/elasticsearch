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
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlConfigurationFunction;
import org.elasticsearch.xpack.esql.session.EsqlConfiguration;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.ql.session.Configuration;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.List;
import java.util.Locale;
import java.util.function.Function;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isDate;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isStringAndExact;
import static org.elasticsearch.xpack.ql.util.DateUtils.UTC_DATE_TIME_FORMATTER;

public class DateFormat extends EsqlConfigurationFunction implements OptionalArgument {

    private final Expression field;
    private final Expression format;

    @FunctionInfo(returnType = "keyword", description = "Returns a string representation of a date, in the provided format.")
    public DateFormat(
        Source source,
        @Param(optional = true, name = "format", type = { "keyword" }, description = "A valid date pattern") Expression format,
        @Param(name = "date", type = { "date" }, description = "Date expression") Expression date,
        Configuration configuration
    ) {
        super(source, date != null ? List.of(format, date) : List.of(format), configuration);
        this.field = date != null ? date : format;
        this.format = date != null ? format : null;
    }

    @Override
    public DataType dataType() {
        return DataTypes.KEYWORD;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = isDate(field, sourceText(), format == null ? FIRST : SECOND);
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

    @Evaluator(extraName = "Constant")
    static BytesRef process(long val, @Fixed DateFormatter formatter) {
        return new BytesRef(formatter.formatMillis(val));
    }

    @Evaluator
    static BytesRef process(long val, BytesRef formatter, @Fixed Locale locale) {
        return process(val, toFormatter(formatter, locale));
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(Function<Expression, ExpressionEvaluator.Factory> toEvaluator) {
        var fieldEvaluator = toEvaluator.apply(field);
        if (format == null) {
            return dvrCtx -> new DateFormatConstantEvaluator(source(), fieldEvaluator.get(dvrCtx), UTC_DATE_TIME_FORMATTER, dvrCtx);
        }
        if (format.dataType() != DataTypes.KEYWORD) {
            throw new IllegalArgumentException("unsupported data type for format [" + format.dataType() + "]");
        }
        if (format.foldable()) {
            DateFormatter formatter = toFormatter(format.fold(), ((EsqlConfiguration) configuration()).locale());
            return dvrCtx -> new DateFormatConstantEvaluator(source(), fieldEvaluator.get(dvrCtx), formatter, dvrCtx);
        }
        var formatEvaluator = toEvaluator.apply(format);
        return dvrCtx -> new DateFormatEvaluator(
            source(),
            fieldEvaluator.get(dvrCtx),
            formatEvaluator.get(dvrCtx),
            ((EsqlConfiguration) configuration()).locale(),
            dvrCtx
        );
    }

    private static DateFormatter toFormatter(Object format, Locale locale) {
        DateFormatter result = format == null ? UTC_DATE_TIME_FORMATTER : DateFormatter.forPattern(((BytesRef) format).utf8ToString());
        return result.withLocale(locale);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new DateFormat(source(), newChildren.get(0), newChildren.size() > 1 ? newChildren.get(1) : null, configuration());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        Expression first = format != null ? format : field;
        Expression second = format != null ? field : null;
        return NodeInfo.create(this, DateFormat::new, first, second, configuration());
    }
}
