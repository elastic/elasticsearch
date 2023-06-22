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
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.planner.Mappable;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.ql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.common.time.DateFormatter.forPattern;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isString;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isStringAndExact;
import static org.elasticsearch.xpack.ql.util.DateUtils.UTC;

public class DateParse extends ScalarFunction implements OptionalArgument, Mappable {

    public static final DateFormatter DEFAULT_FORMATTER = toFormatter(new BytesRef("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"), UTC);
    private final Expression field;
    private final Expression format;

    public DateParse(Source source, Expression field, Expression format) {
        super(source, format != null ? Arrays.asList(field, format) : Arrays.asList(field));
        this.field = field;
        this.format = format;
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

        TypeResolution resolution = isString(field, sourceText(), FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }
        if (format != null) {
            resolution = isStringAndExact(format, sourceText(), SECOND);
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

    @Override
    public Object fold() {
        return Mappable.super.fold();
    }

    @Evaluator(extraName = "Constant", warnExceptions = { IllegalArgumentException.class })
    public static long process(BytesRef val, @Fixed DateFormatter formatter) throws IllegalArgumentException {
        String dateString = val.utf8ToString();
        return formatter.parseMillis(dateString);
    }

    @Evaluator(warnExceptions = { IllegalArgumentException.class })
    static long process(BytesRef val, BytesRef formatter, @Fixed ZoneId zoneId) throws IllegalArgumentException {
        return process(val, toFormatter(formatter, zoneId));
    }

    @Override
    public Supplier<EvalOperator.ExpressionEvaluator> toEvaluator(
        Function<Expression, Supplier<EvalOperator.ExpressionEvaluator>> toEvaluator
    ) {
        ZoneId zone = UTC; // TODO session timezone?
        Supplier<EvalOperator.ExpressionEvaluator> fieldEvaluator = toEvaluator.apply(field);
        if (format == null) {
            return () -> new DateParseConstantEvaluator(source(), fieldEvaluator.get(), DEFAULT_FORMATTER);
        }
        if (format.dataType() != DataTypes.KEYWORD) {
            throw new IllegalArgumentException("unsupported data type for date_parse [" + format.dataType() + "]");
        }
        if (format.foldable()) {
            try {
                DateFormatter formatter = toFormatter(format.fold(), zone);
                return () -> new DateParseConstantEvaluator(source(), fieldEvaluator.get(), formatter);
            } catch (IllegalArgumentException e) {
                throw new EsqlIllegalArgumentException(e, "invalid date patter for [{}]: {}", sourceText(), e.getMessage());
            }
        }
        Supplier<EvalOperator.ExpressionEvaluator> formatEvaluator = toEvaluator.apply(format);
        return () -> new DateParseEvaluator(source(), fieldEvaluator.get(), formatEvaluator.get(), zone);
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
        return NodeInfo.create(this, DateParse::new, field, format);
    }

    @Override
    public ScriptTemplate asScript() {
        throw new UnsupportedOperationException();
    }
}
