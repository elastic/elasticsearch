/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractAggregationTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class FirstValueTests extends AbstractAggregationTestCase {
    public FirstValueTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        var suppliers = new ArrayList<TestCaseSupplier>();

        suppliers.add(
            new TestCaseSupplier(
                "first(long, long): long",
                List.of(DataType.LONG, DataType.LONG),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        TestCaseSupplier.TypedData.multiRow(List.of(1000L, 999L), DataType.LONG, "field"),
                        TestCaseSupplier.TypedData.multiRow(List.of(2025L, 2026L), DataType.LONG, "timestamp")
                    ),
                    "FirstValue[field=Attribute[channel=0], timestamp=Attribute[channel=1]]",
                    DataType.LONG,
                    equalTo(1000L)
                )
            )
        );
        suppliers.add(
            new TestCaseSupplier(
                "first(keyword, long): keyword",
                List.of(DataType.KEYWORD, DataType.LONG),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        TestCaseSupplier.TypedData.multiRow(List.of(new BytesRef("1000"), new BytesRef("999")), DataType.KEYWORD, "field"),
                        TestCaseSupplier.TypedData.multiRow(List.of(2025L, 2026L), DataType.LONG, "timestamp")
                    ),
                    "FirstValue[field=Attribute[channel=0], timestamp=Attribute[channel=1]]",
                    DataType.KEYWORD,
                    equalTo(new BytesRef("1000"))
                )
            )
        );

        return parameterSuppliersFromTypedDataWithDefaultChecksNoErrors(suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new FirstValue(source, args.get(0), args.get(1));
    }
}
