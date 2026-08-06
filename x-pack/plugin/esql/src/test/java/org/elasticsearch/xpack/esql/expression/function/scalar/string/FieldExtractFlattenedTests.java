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
import static org.hamcrest.Matchers.nullValue;

public class FieldExtractFlattenedTests extends AbstractScalarFunctionTestCase {

    public FieldExtractFlattenedTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        for (DataType pathType : List.of(DataType.KEYWORD, DataType.TEXT)) {
            suppliers.add(new TestCaseSupplier("2 args match " + pathType, List.of(DataType.FLATTENED, pathType), () -> {
                return new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(
                            new BytesRef("{\"a.b.c\":1, \"a.b.d\":2, \"a.g\":4, \"d.e.f\":3}"),
                            DataType.FLATTENED,
                            "field"
                        ),
                        new TestCaseSupplier.TypedData(new BytesRef("a"), pathType, "path")
                    ),
                    "FieldExtractFlattenedNoKeyEvaluator[flattenedJson=Attribute[channel=0], path=Attribute[channel=1]]",
                    DataType.FLATTENED,
                    equalTo(new BytesRef("{\"c\":1,\"d\":2}"))
                );
            }));

            suppliers.add(new TestCaseSupplier("Missing path " + pathType, List.of(DataType.FLATTENED, pathType), () -> {
                return new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(new BytesRef("{\"a.b.c\":1}"), DataType.FLATTENED, "field"),
                        new TestCaseSupplier.TypedData(new BytesRef("x"), pathType, "path")
                    ),
                    "FieldExtractFlattenedNoKeyEvaluator[flattenedJson=Attribute[channel=0], path=Attribute[channel=1]]",
                    DataType.FLATTENED,
                    nullValue()
                );
            }));

            for (DataType keyType : List.of(DataType.KEYWORD, DataType.TEXT)) {
                suppliers.add(
                    new TestCaseSupplier("3 args match " + pathType + " " + keyType, List.of(DataType.FLATTENED, pathType, keyType), () -> {
                        return new TestCaseSupplier.TestCase(
                            List.of(
                                new TestCaseSupplier.TypedData(
                                    new BytesRef("{\"a.b.c\":1, \"a.b.d\":2, \"d.e.f\":3}"),
                                    DataType.FLATTENED,
                                    "field"
                                ),
                                new TestCaseSupplier.TypedData(new BytesRef("a"), pathType, "path"),
                                new TestCaseSupplier.TypedData(new BytesRef("key"), keyType, "key")
                            ),
                            "FieldExtractFlattenedEvaluator[flattenedJson=Attribute[channel=0], "
                                + "path=Attribute[channel=1], injectedKey=Attribute[channel=2]]",
                            DataType.FLATTENED,
                            equalTo(new BytesRef("{\"c\":1,\"d\":2,\"key\":\"b\"}"))
                        );
                    })
                );
            }
        }

        suppliers.add(new TestCaseSupplier("Collision match", List.of(DataType.FLATTENED, DataType.KEYWORD, DataType.KEYWORD), () -> {
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(
                        new BytesRef("{\"a.b.c\":1, \"a.b.key\":\"original\", \"d.e.f\":3}"),
                        DataType.FLATTENED,
                        "field"
                    ),
                    new TestCaseSupplier.TypedData(new BytesRef("a"), DataType.KEYWORD, "path"),
                    new TestCaseSupplier.TypedData(new BytesRef("key"), DataType.KEYWORD, "key")
                ),
                "FieldExtractFlattenedEvaluator[flattenedJson=Attribute[channel=0], "
                    + "path=Attribute[channel=1], injectedKey=Attribute[channel=2]]",
                DataType.FLATTENED,
                equalTo(new BytesRef("{\"c\":1,\"key\":\"b\"}"))
            );
        }));

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        if (args.size() == 2) {
            return new FieldExtractFlattened(source, args.get(0), args.get(1), null);
        }
        return new FieldExtractFlattened(source, args.get(0), args.get(1), args.get(2));
    }
}
