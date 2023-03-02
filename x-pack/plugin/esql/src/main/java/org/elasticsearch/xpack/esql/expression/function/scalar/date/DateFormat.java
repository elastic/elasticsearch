/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.planner.Mappable;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.ql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isDate;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isStringAndExact;
import static org.elasticsearch.xpack.ql.util.DateUtils.UTC_DATE_TIME_FORMATTER;

public class DateFormat extends ScalarFunction implements OptionalArgument, Mappable {

    private final Expression field;
    private final Expression format;

    public DateFormat(Source source, Expression field, Expression format) {
        super(source, format != null ? Arrays.asList(field, format) : Arrays.asList(field));
        this.field = field;
        this.format = format;
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

        TypeResolution resolution = isDate(field, sourceText(), FIRST);
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
        return process((Long) field.fold(), foldedFormatter());
    }

    private DateFormatter foldedFormatter() {
        if (format == null) {
            return UTC_DATE_TIME_FORMATTER;
        } else {
            return DateFormatter.forPattern((String) format.fold());
        }
    }

    private static BytesRef process(Long fieldVal, DateFormatter formatter) {
        if (fieldVal == null) {
            return null;
        } else {
            return new BytesRef(formatter.formatMillis(fieldVal));
        }
    }

    record DateFormatEvaluator(EvalOperator.ExpressionEvaluator exp, EvalOperator.ExpressionEvaluator formatEvaluator)
        implements
            EvalOperator.ExpressionEvaluator {
        @Override
        public Object computeRow(Page page, int pos) {
            return process(((Long) exp.computeRow(page, pos)), toFormatter(formatEvaluator.computeRow(page, pos)));
        }
    }

    record ConstantDateFormatEvaluator(EvalOperator.ExpressionEvaluator exp, DateFormatter formatter)
        implements
            EvalOperator.ExpressionEvaluator {
        @Override
        public Object computeRow(Page page, int pos) {
            return process(((Long) exp.computeRow(page, pos)), formatter);
        }
    }

    @Override
    public Supplier<EvalOperator.ExpressionEvaluator> toEvaluator(
        Function<Expression, Supplier<EvalOperator.ExpressionEvaluator>> toEvaluator
    ) {
        Supplier<EvalOperator.ExpressionEvaluator> fieldEvaluator = toEvaluator.apply(field);
        if (format == null) {
            return () -> new ConstantDateFormatEvaluator(fieldEvaluator.get(), UTC_DATE_TIME_FORMATTER);
        }
        if (format.dataType() != DataTypes.KEYWORD) {
            throw new IllegalArgumentException("unsupported data type for format [" + format.dataType() + "]");
        }
        if (format.foldable()) {
            DateFormatter formatter = toFormatter(format.fold());
            return () -> new ConstantDateFormatEvaluator(fieldEvaluator.get(), formatter);
        }
        Supplier<EvalOperator.ExpressionEvaluator> formatEvaluator = toEvaluator.apply(format);
        return () -> new DateFormatEvaluator(fieldEvaluator.get(), formatEvaluator.get());
    }

    private static DateFormatter toFormatter(Object format) {
        return format == null ? UTC_DATE_TIME_FORMATTER : DateFormatter.forPattern(((BytesRef) format).utf8ToString());
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new DateFormat(source(), newChildren.get(0), newChildren.size() > 1 ? newChildren.get(1) : null);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, DateFormat::new, field, format);
    }

    @Override
    public ScriptTemplate asScript() {
        throw new UnsupportedOperationException();
    }
}
