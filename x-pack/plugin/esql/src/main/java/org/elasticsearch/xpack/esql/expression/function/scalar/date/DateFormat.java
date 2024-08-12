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
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlConfigurationFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isDate;
import static org.elasticsearch.xpack.esql.expression.EsqlTypeResolutions.isStringAndExact;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.DEFAULT_DATE_TIME_FORMATTER;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateTimeToString;

public class DateFormat extends EsqlConfigurationFunction implements OptionalArgument {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "DateFormat",
        DateFormat::new
    );

    private final Expression field;
    private final Expression format;

    @FunctionInfo(
        returnType = "keyword",
        description = "Returns a string representation of a date, in the provided format.",
        examples = @Example(file = "date", tag = "docsDateFormat")
    )
    public DateFormat(
        Source source,
        @Param(optional = true, name = "dateFormat", type = { "keyword", "text" }, description = """
            Date format (optional).  If no format is specified, the `yyyy-MM-dd'T'HH:mm:ss.SSSZ` format is used.
            If `null`, the function returns `null`.""") Expression format,
        @Param(name = "date", type = { "date" }, description = "Date expression. If `null`, the function returns `null`.") Expression date,
        Configuration configuration
    ) {
        super(source, date != null ? List.of(format, date) : List.of(format), configuration);
        this.field = date != null ? date : format;
        this.format = date != null ? format : null;
    }

    private DateFormat(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readOptionalNamedWriteable(Expression.class),
            ((PlanStreamInput) in).configuration()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(children().get(0));
        out.writeOptionalNamedWriteable(children().size() == 2 ? children().get(1) : null);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    Expression field() {
        return field;
    }

    Expression format() {
        return format;
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

        TypeResolution resolution;
        if (format != null) {
            resolution = isStringAndExact(format, sourceText(), FIRST);
            if (resolution.unresolved()) {
                return resolution;
            }
        }

        resolution = isDate(field, sourceText(), format == null ? FIRST : SECOND);
        if (resolution.unresolved()) {
            return resolution;
        }

        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public boolean foldable() {
        return field.foldable() && (format == null || format.foldable());
    }

    @Evaluator(extraName = "Constant")
    static BytesRef process(long val, @Fixed DateFormatter formatter) {
        return new BytesRef(dateTimeToString(val, formatter));
    }

    @Evaluator
    static BytesRef process(long val, BytesRef formatter, @Fixed Locale locale) {
        return new BytesRef(dateTimeToString(val, toFormatter(formatter, locale)));
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(Function<Expression, ExpressionEvaluator.Factory> toEvaluator) {
        var fieldEvaluator = toEvaluator.apply(field);
        if (format == null) {
            return new DateFormatConstantEvaluator.Factory(source(), fieldEvaluator, DEFAULT_DATE_TIME_FORMATTER);
        }
        if (DataType.isString(format.dataType()) == false) {
            throw new IllegalArgumentException("unsupported data type for format [" + format.dataType() + "]");
        }
        if (format.foldable()) {
            DateFormatter formatter = toFormatter(format.fold(), configuration().locale());
            return new DateFormatConstantEvaluator.Factory(source(), fieldEvaluator, formatter);
        }
        var formatEvaluator = toEvaluator.apply(format);
        return new DateFormatEvaluator.Factory(source(), fieldEvaluator, formatEvaluator, configuration().locale());
    }

    private static DateFormatter toFormatter(Object format, Locale locale) {
        DateFormatter result = format == null ? DEFAULT_DATE_TIME_FORMATTER : DateFormatter.forPattern(((BytesRef) format).utf8ToString());
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
