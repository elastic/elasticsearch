/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class MvConcatTests extends AbstractScalarFunctionTestCase {
    public MvConcatTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        for (DataType fieldType : DataType.types()) {
            if (DataType.isString(fieldType) == false) {
                continue;
            }
            for (DataType delimType : DataType.types()) {
                if (DataType.isString(delimType) == false) {
                    continue;
                }
                for (int l = 1; l < 10; l++) {
                    int length = l;
                    suppliers.add(new TestCaseSupplier(fieldType + "/" + l + " " + delimType, List.of(fieldType, delimType), () -> {
                        String delim = randomAlphaOfLengthBetween(0, 5);
                        List<BytesRef> data = new ArrayList<>(length);
                        String expected = null;
                        for (int i = 0; i < length; i++) {
                            String value = randomRealisticUnicodeOfLengthBetween(0, 10);
                            data.add(new BytesRef(value));
                            if (expected == null) {
                                expected = value;
                            } else {
                                expected += delim + value;
                            }
                        }
                        return new TestCaseSupplier.TestCase(
                            List.of(
                                new TestCaseSupplier.TypedData(data, fieldType, "field"),
                                new TestCaseSupplier.TypedData(new BytesRef(delim), delimType, "delim")
                            ),
                            "MvConcat[field=Attribute[channel=0], delim=Attribute[channel=1]]",
                            DataType.KEYWORD,
                            equalTo(new BytesRef(expected))
                        );
                    }));
                }
            }
        }
        return parameterSuppliersFromTypedDataWithDefaultChecks(false, suppliers, (v, p) -> "string");
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new MvConcat(source, args.get(0), args.get(1));
    }
}
