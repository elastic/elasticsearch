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
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class DateFormatTests extends AbstractScalarFunctionTestCase {
    public DateFormatTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return parameterSuppliersFromTypedData(
            List.of(
                new TestCaseSupplier(
                    List.of(DataTypes.KEYWORD, DataTypes.DATETIME),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(new BytesRef("yyyy"), DataTypes.KEYWORD, "formatter"),
                            new TestCaseSupplier.TypedData(1687944333000L, DataTypes.DATETIME, "val")
                        ),
                        "DateFormatEvaluator[val=Attribute[channel=1], formatter=Attribute[channel=0], locale=en_US]",
                        DataTypes.KEYWORD,
                        equalTo(BytesRefs.toBytesRef("2023"))
                    )
                ),
                new TestCaseSupplier(
                    List.of(DataTypes.TEXT, DataTypes.DATETIME),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(new BytesRef("yyyy"), DataTypes.TEXT, "formatter"),
                            new TestCaseSupplier.TypedData(1687944333000L, DataTypes.DATETIME, "val")
                        ),
                        "DateFormatEvaluator[val=Attribute[channel=1], formatter=Attribute[channel=0], locale=en_US]",
                        DataTypes.KEYWORD,
                        equalTo(BytesRefs.toBytesRef("2023"))
                    )
                )
            )
        );
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new DateFormat(source, args.get(0), args.get(1), EsqlTestUtils.TEST_CFG);
    }

    @Override
    protected List<ArgumentSpec> argSpec() {
        return List.of(required(strings()), required(DataTypes.DATETIME));
    }

    @Override
    protected DataType expectedType(List<DataType> argTypes) {
        return DataTypes.KEYWORD;
    }
}
