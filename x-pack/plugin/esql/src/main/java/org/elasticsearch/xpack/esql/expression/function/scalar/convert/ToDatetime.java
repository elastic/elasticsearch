/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.ann.ConvertEvaluator;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateTimeToLong;
import static org.elasticsearch.xpack.ql.type.DataTypes.DATETIME;
import static org.elasticsearch.xpack.ql.type.DataTypes.DOUBLE;
import static org.elasticsearch.xpack.ql.type.DataTypes.INTEGER;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.ql.type.DataTypes.LONG;
import static org.elasticsearch.xpack.ql.type.DataTypes.TEXT;
import static org.elasticsearch.xpack.ql.type.DataTypes.UNSIGNED_LONG;

public class ToDatetime extends AbstractConvertFunction {

    private static final Map<DataType, BuildFactory> EVALUATORS = Map.ofEntries(
        Map.entry(DATETIME, (field, source) -> field),
        Map.entry(LONG, (field, source) -> field),
        Map.entry(KEYWORD, ToDatetimeFromStringEvaluator.Factory::new),
        Map.entry(TEXT, ToDatetimeFromStringEvaluator.Factory::new),
        Map.entry(DOUBLE, ToLongFromDoubleEvaluator.Factory::new),
        Map.entry(UNSIGNED_LONG, ToLongFromUnsignedLongEvaluator.Factory::new),
        Map.entry(INTEGER, ToLongFromIntEvaluator.Factory::new) // CastIntToLongEvaluator would be a candidate, but not MV'd
    );

    @FunctionInfo(
        returnType = "date",
        description = """
            Converts an input value to a date value.
            A string will only be successfully converted if it's respecting the format `yyyy-MM-dd'T'HH:mm:ss.SSS'Z'`.
            To convert dates in other formats, use <<esql-date_parse>>.""",
        examples = {
            @Example(file = "date", tag = "to_datetime-str", explanation = """
                Note that in this example, the last value in the source multi-valued field has not been converted.
                The reason being that if the date format is not respected, the conversion will result in a *null* value.
                When this happens a _Warning_ header is added to the response.
                The header will provide information on the source of the failure:

                `"Line 1:112: evaluation of [TO_DATETIME(string)] failed, treating result as null. "Only first 20 failures recorded."`

                A following header will contain the failure reason and the offending value:

                `"java.lang.IllegalArgumentException: failed to parse date field [1964-06-02 00:00:00]
                with format [yyyy-MM-dd'T'HH:mm:ss.SSS'Z']"`
                """),
            @Example(
                description = """
                    If the input parameter is of a numeric type,
                    its value will be interpreted as milliseconds since the {wikipedia}/Unix_time[Unix epoch]. For example:""",
                file = "date",
                tag = "to_datetime-int"
            ) }
    )
    public ToDatetime(
        Source source,
        @Param(
            name = "field",
            type = { "date", "keyword", "text", "double", "long", "unsigned_long", "integer" },
            description = "Input value. The input can be a single- or multi-valued column or an expression."
        ) Expression field
    ) {
        super(source, field);
    }

    @Override
    protected Map<DataType, BuildFactory> factories() {
        return EVALUATORS;
    }

    @Override
    public DataType dataType() {
        return DATETIME;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ToDatetime(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ToDatetime::new, field());
    }

    @ConvertEvaluator(extraName = "FromString", warnExceptions = { IllegalArgumentException.class })
    static long fromKeyword(BytesRef in) {
        return dateTimeToLong(in.utf8ToString());
    }
}
