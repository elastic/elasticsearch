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
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.common.time.DateFormatter.forPattern;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;
import static org.elasticsearch.xpack.esql.expression.EsqlTypeResolutions.isStringAndExact;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.DEFAULT_DATE_TIME_FORMATTER;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateTimeToLong;

public class DateParse extends EsqlScalarFunction implements OptionalArgument {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "DateParse",
        DateParse::new
    );

    private final Expression field;
    private final Expression format;

    @FunctionInfo(
        returnType = "date",
        description = "Returns a date by parsing the second argument using the format specified in the first argument.",
        examples = @Example(file = "docs", tag = "dateParse")
    )
    public DateParse(
        Source source,
        @Param(name = "datePattern", type = { "keyword", "text" }, description = """
            The date format. Refer to the
            {javadoc14}/java.base/java/time/format/DateTimeFormatter.html[`DateTimeFormatter` documentation] for the syntax.
            If `null`, the function returns `null`.""", optional = true) Expression first,
        @Param(
            name = "dateString",
            type = { "keyword", "text" },
            description = "Date expression as a string. If `null` or an empty string, the function returns `null`."
        ) Expression second
    ) {
        super(source, second != null ? List.of(first, second) : List.of(first));
        this.field = second != null ? second : first;
        this.format = second != null ? first : null;
    }

    private DateParse(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readOptionalNamedWriteable(Expression.class)
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

    @Override
    public DataType dataType() {
        return DataType.DATETIME;
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

        resolution = isString(field, sourceText(), format != null ? SECOND : FIRST);
        if (resolution.unresolved()) {
            return resolution;
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
    static long process(BytesRef val, BytesRef formatter) throws IllegalArgumentException {
        return dateTimeToLong(val.utf8ToString(), toFormatter(formatter));
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        ExpressionEvaluator.Factory fieldEvaluator = toEvaluator.apply(field);
        if (format == null) {
            return new DateParseConstantEvaluator.Factory(source(), fieldEvaluator, DEFAULT_DATE_TIME_FORMATTER);
        }
        if (DataType.isString(format.dataType()) == false) {
            throw new IllegalArgumentException("unsupported data type for date_parse [" + format.dataType() + "]");
        }
        if (format.foldable()) {
            try {
                DateFormatter formatter = toFormatter(format.fold(toEvaluator.foldCtx()));
                return new DateParseConstantEvaluator.Factory(source(), fieldEvaluator, formatter);
            } catch (IllegalArgumentException e) {
                throw new InvalidArgumentException(e, "invalid date pattern for [{}]: {}", sourceText(), e.getMessage());
            }
        }
        ExpressionEvaluator.Factory formatEvaluator = toEvaluator.apply(format);
        return new DateParseEvaluator.Factory(source(), fieldEvaluator, formatEvaluator);
    }

    private static DateFormatter toFormatter(Object format) {
        return forPattern(((BytesRef) format).utf8ToString());
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
