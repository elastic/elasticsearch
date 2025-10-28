/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

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

public class UriPartsTests extends AbstractScalarFunctionTestCase {
    public UriPartsTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        for (DataType sType : DataType.stringTypes()) {
            for (DataType dType : DataType.stringTypes()) {
                suppliers.add(
                    new TestCaseSupplier(
                        "uri_parts test " + sType.toString() + " " + dType.toString(),
                        List.of(sType, dType),
                        () -> new TestCaseSupplier.TestCase(
                            List.of(
                                new TestCaseSupplier.TypedData(new BytesRef("http://example.org/sub/path"), sType, "urlString"),
                                new TestCaseSupplier.TypedData(new BytesRef("domain"), dType, "field")
                            ),
                            "UriPartsEvaluator[urlString=Attribute[channel=0], field=Attribute[channel=1]]",
                            DataType.KEYWORD,
                            equalTo(new BytesRef("example.org"))
                        )
                    )
                );
            }
        }

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new UriParts(source, args.get(0), args.get(1));
    }
}
