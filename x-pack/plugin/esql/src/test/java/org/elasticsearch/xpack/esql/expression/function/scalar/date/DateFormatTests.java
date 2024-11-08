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
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractConfigurationFunctionTestCase;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class DateFormatTests extends AbstractConfigurationFunctionTestCase {
    public DateFormatTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return parameterSuppliersFromTypedDataWithDefaultChecks(
            true,
            List.of(
                new TestCaseSupplier(
                    List.of(DataType.KEYWORD, DataType.DATETIME),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(new BytesRef("yyyy"), DataType.KEYWORD, "formatter"),
                            new TestCaseSupplier.TypedData(1687944333000L, DataType.DATETIME, "val")
                        ),
                        "DateFormatEvaluator[val=Attribute[channel=1], formatter=Attribute[channel=0], locale=en_US]",
                        DataType.KEYWORD,
                        equalTo(BytesRefs.toBytesRef("2023"))
                    )
                ),
                new TestCaseSupplier(
                    List.of(DataType.TEXT, DataType.DATETIME),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(new BytesRef("yyyy"), DataType.TEXT, "formatter"),
                            new TestCaseSupplier.TypedData(1687944333000L, DataType.DATETIME, "val")
                        ),
                        "DateFormatEvaluator[val=Attribute[channel=1], formatter=Attribute[channel=0], locale=en_US]",
                        DataType.KEYWORD,
                        equalTo(BytesRefs.toBytesRef("2023"))
                    )
                )
            ),
            (v, p) -> switch (p) {
                case 0 -> "string";
                case 1 -> "datetime";
                default -> "";
            }
        );
    }

    @Override
    protected Expression buildWithConfiguration(Source source, List<Expression> args, Configuration configuration) {
        return new DateFormat(source, args.get(0), args.get(1), configuration);
    }
}
