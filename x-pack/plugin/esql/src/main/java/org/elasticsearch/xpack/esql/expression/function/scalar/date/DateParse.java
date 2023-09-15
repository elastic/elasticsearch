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
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
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

import static org.elasticsearch.common.time.DateFormatter.forPattern;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isString;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isStringAndExact;
import static org.elasticsearch.xpack.ql.util.DateUtils.UTC;

public class DateParse extends ScalarFunction implements OptionalArgument, EvaluatorMapper {

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
        return EvaluatorMapper.super.fold();
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
    public ExpressionEvaluator.Factory toEvaluator(Function<Expression, ExpressionEvaluator.Factory> toEvaluator) {
        ZoneId zone = UTC; // TODO session timezone?
        ExpressionEvaluator.Factory fieldEvaluator = toEvaluator.apply(field);
        if (format == null) {
            return dvrCtx -> new DateParseConstantEvaluator(source(), fieldEvaluator.get(dvrCtx), DEFAULT_FORMATTER, dvrCtx);
        }
        if (format.dataType() != DataTypes.KEYWORD) {
            throw new IllegalArgumentException("unsupported data type for date_parse [" + format.dataType() + "]");
        }
        if (format.foldable()) {
            try {
                DateFormatter formatter = toFormatter(format.fold(), zone);
                return dvrCtx -> new DateParseConstantEvaluator(source(), fieldEvaluator.get(dvrCtx), formatter, dvrCtx);
            } catch (IllegalArgumentException e) {
                throw new EsqlIllegalArgumentException(e, "invalid date pattern for [{}]: {}", sourceText(), e.getMessage());
            }
        }
        ExpressionEvaluator.Factory formatEvaluator = toEvaluator.apply(format);
        return dvrCtx -> new DateParseEvaluator(source(), fieldEvaluator.get(dvrCtx), formatEvaluator.get(dvrCtx), zone, dvrCtx);
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
        throw new UnsupportedOperationException("functions do not support scripting");
    }
}
