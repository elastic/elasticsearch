/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class AutoBucketTests extends AbstractScalarFunctionTestCase {
    public AutoBucketTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return parameterSuppliersFromTypedData(List.of(new TestCaseSupplier("Autobucket Single date", () -> {
            List<TestCaseSupplier.TypedData> args = List.of(
                new TestCaseSupplier.TypedData(
                    DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2023-02-17T09:00:00.00Z"),
                    DataTypes.DATETIME,
                    "arg"
                )
            );
            return new TestCaseSupplier.TestCase(
                args,
                "DateTruncEvaluator[fieldVal=Attribute[channel=0], rounding=Rounding[DAY_OF_MONTH in Z][fixed to midnight]]",
                DataTypes.DATETIME,
                resultsMatcher(args)
            );
        })));
    }

    private Expression build(Source source, Expression arg) {
        Literal from;
        Literal to;
        if (arg.dataType() == DataTypes.DATETIME) {
            from = new Literal(Source.EMPTY, new BytesRef("2023-02-01T00:00:00.00Z"), DataTypes.KEYWORD);
            to = new Literal(Source.EMPTY, new BytesRef("2023-03-01T00:00:00.00Z"), DataTypes.KEYWORD);
        } else {
            from = new Literal(Source.EMPTY, 0, DataTypes.DOUBLE);
            to = new Literal(Source.EMPTY, 1000, DataTypes.DOUBLE);
        }
        return new AutoBucket(source, arg, new Literal(Source.EMPTY, 50, DataTypes.INTEGER), from, to);
    }

    @Override
    protected DataType expectedType(List<DataType> argTypes) {
        if (argTypes.get(0).isNumeric()) {
            return DataTypes.DOUBLE;
        }
        return argTypes.get(0);
    }

    private static Matcher<Object> resultsMatcher(List<TestCaseSupplier.TypedData> typedData) {
        long millis = ((Number) typedData.get(0).data()).longValue();
        return equalTo(Rounding.builder(Rounding.DateTimeUnit.DAY_OF_MONTH).build().prepareForUnknown().round(millis));
    }

    @Override
    protected List<ArgumentSpec> argSpec() {
        DataType[] numerics = numerics();
        DataType[] all = new DataType[numerics.length + 1];
        all[0] = DataTypes.DATETIME;
        System.arraycopy(numerics, 0, all, 1, numerics.length);
        return List.of(required(all));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return build(source, args.get(0));
    }

    @Override
    protected Matcher<String> badTypeError(List<ArgumentSpec> spec, int badArgPosition, DataType badArgType) {
        return equalTo("first argument of [exp] must be [datetime or numeric], found value [arg0] type [" + badArgType.typeName() + "]");
    }
}
