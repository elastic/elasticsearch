/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class DateParseTests extends AbstractScalarFunctionTestCase {
    public DateParseTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return parameterSuppliersFromTypedData(
            List.of(
                new TestCaseSupplier(
                    "Basic Case",
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(new BytesRef("yyyy-MM-dd"), DataTypes.KEYWORD, "second"),
                            new TestCaseSupplier.TypedData(new BytesRef("2023-05-05"), DataTypes.KEYWORD, "first")
                        ),
                        "DateParseEvaluator[val=Attribute[channel=1], formatter=Attribute[channel=0], zoneId=Z]",
                        DataTypes.DATETIME,
                        equalTo(1683244800000L)
                    )
                ),
                new TestCaseSupplier(
                    "With Text",
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(new BytesRef("yyyy-MM-dd"), DataTypes.KEYWORD, "second"),
                            new TestCaseSupplier.TypedData(new BytesRef("2023-05-05"), DataTypes.TEXT, "first")
                        ),
                        "DateParseEvaluator[val=Attribute[channel=1], formatter=Attribute[channel=0], zoneId=Z]",
                        DataTypes.DATETIME,
                        equalTo(1683244800000L)
                    )
                )
            )
        );
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new DateParse(source, args.get(0), args.size() > 1 ? args.get(1) : null);
    }

    @Override
    protected List<ArgumentSpec> argSpec() {
        return List.of(optional(strings()), required(strings()));
    }

    @Override
    protected DataType expectedType(List<DataType> argTypes) {
        return DataTypes.DATETIME;
    }
}
