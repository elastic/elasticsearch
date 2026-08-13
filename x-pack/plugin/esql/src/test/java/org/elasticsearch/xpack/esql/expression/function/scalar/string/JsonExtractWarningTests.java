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

import static org.hamcrest.Matchers.nullValue;

/**
 * Warning-emitting parameterized cases for {@link JsonExtract}: invalid JSON, missing paths, array
 * bounds, type mismatches, unsupported JSONPath features, and the {@code _source}-is-null warning
 * (only fires when the first argument is the SOURCE-typed {@code _source} field). Split off
 * {@link JsonExtractTests} so the {@code repeat-changed-tests} runner stays under 512m heap.
 */
public class JsonExtractWarningTests extends AbstractScalarFunctionTestCase {

    public JsonExtractWarningTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        // Type-coverage suppliers — every declared str/path type pair gets a warning case so the
        // @AfterClass testFunctionInfo check sees the full set this function declares.
        for (DataType jsonType : DataType.stringTypes()) {
            for (DataType pathType : DataType.stringTypes()) {
                suppliers.add(
                    warningCase(
                        "missing path " + jsonType.esNameIfPossible() + "/" + pathType.esNameIfPossible(),
                        jsonType,
                        pathType,
                        "{\"a\":1}",
                        "b",
                        "path [b] does not exist"
                    )
                );
            }
        }

        // Missing path / invalid input
        suppliers.add(warningCase("invalid JSON", "not valid json", "field", "invalid JSON input"));
        suppliers.add(warningCase("empty input", "", "$", "empty JSON input"));
        suppliers.add(warningCase("empty object path miss", "{}", "anything", "path [anything] does not exist"));

        // Array bounds and type mismatches
        suppliers.add(warningCase("array index out of bounds", "{\"a\":[1,2,3]}", "a[5]", "array index out of bounds"));
        suppliers.add(foldingWarningCase("negative array index", "{\"a\":[1,2,3]}", "a[-1]", "array index out of bounds"));
        suppliers.add(warningCase("key into array", "{\"a\":[1,2]}", "a.b", "path [a.b] does not exist"));
        suppliers.add(warningCase("index into object", "{\"a\":{\"b\":1}}", "a[0]", "path [a[0]] does not exist"));
        suppliers.add(warningCase("key into scalar", "{\"a\":42}", "a.b", "path [a.b] does not exist"));

        // Unsupported JSONPath features
        suppliers.add(warningCase("wildcard .*", "{\"a\":1,\"b\":2}", "*", "path [*] does not exist"));
        suppliers.add(
            foldingWarningCase(
                "wildcard [*]",
                "[1,2,3]",
                "$[*]",
                "Invalid JSON path [$[*]]: expected integer array index, got [*] at position 1"
            )
        );
        suppliers.add(
            foldingWarningCase(
                "recursive descent $..",
                "{\"a\":{\"name\":1}}",
                "$..name",
                "Invalid JSON path [$..name]: path cannot start with a dot at position 2"
            )
        );
        suppliers.add(
            foldingWarningCase(
                "recursive descent bare ..",
                "{\"a\":{\"name\":1}}",
                "..name",
                "Invalid JSON path [..name]: path cannot start with a dot at position 0"
            )
        );
        suppliers.add(
            foldingWarningCase(
                "array slice",
                "{\"arr\":[1,2,3,4]}",
                "arr[0:3]",
                "Invalid JSON path [arr[0:3]]: expected integer array index, got [0:3] at position 3"
            )
        );
        suppliers.add(
            foldingWarningCase(
                "array slice with step",
                "{\"arr\":[1,2,3,4]}",
                "arr[::2]",
                "Invalid JSON path [arr[::2]]: expected integer array index, got [::2] at position 3"
            )
        );
        suppliers.add(
            foldingWarningCase(
                "filter expression",
                "{\"items\":[{\"price\":5}]}",
                "items[?(@.price<10)]",
                "Invalid JSON path [items[?(@.price<10)]]: expected integer array index, got [?(@.price<10)] at position 5"
            )
        );
        suppliers.add(
            foldingWarningCase(
                "union [0,1]",
                "{\"arr\":[1,2,3]}",
                "arr[0,1]",
                "Invalid JSON path [arr[0,1]]: expected integer array index, got [0,1] at position 3"
            )
        );

        // Multi-value path: the standard MV single-value warning fires for the path argument.
        suppliers.add(
            new TestCaseSupplier(
                "mv path",
                types(DataType.KEYWORD, DataType.KEYWORD),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(new BytesRef("{\"name\":\"Alice\"}"), DataType.KEYWORD, "str"),
                        new TestCaseSupplier.TypedData(List.of(new BytesRef("name"), new BytesRef("age")), DataType.KEYWORD, "path")
                    ),
                    "JsonExtractEvaluator[str=Attribute[channel=0], path=Attribute[channel=1]]",
                    DataType.KEYWORD,
                    nullValue()
                ).withWarning("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.")
                    .withWarning("Line 1:1: java.lang.IllegalArgumentException: single-value function encountered multi-value")
            )
        );

        // Null _source warning — only fires when str is SOURCE-typed and the input bytes are null.
        // Routes through JsonExtractSourceEvaluator instead of JsonExtractEvaluator.
        List<TestCaseSupplier> nullSourceSuppliers = new ArrayList<>();
        for (DataType pathType : DataType.stringTypes()) {
            nullSourceSuppliers.add(
                new TestCaseSupplier(
                    "null source " + TestCaseSupplier.nameFromTypes(types(DataType.SOURCE, pathType)),
                    types(DataType.SOURCE, pathType),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(null, DataType.SOURCE, "str"),
                            new TestCaseSupplier.TypedData(new BytesRef("name"), pathType, "path")
                        ),
                        "JsonExtractSourceEvaluator[strBlock=Attribute[channel=0], pathBlock=Attribute[channel=1]]",
                        DataType.KEYWORD,
                        nullValue()
                    ).withWarning("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.")
                        .withWarning(
                            "Line 1:1: java.lang.IllegalStateException: "
                                + "_source is null; this typically means the index has _source disabled in its mapping"
                        )
                )
            );
        }

        // Auto-null pass over the non-SOURCE suppliers; append SOURCE ones (which carry their own
        // warning expectations the auto pass can't express).
        List<TestCaseSupplier> withNullCases = anyNullIsNull(true, suppliers);
        withNullCases.addAll(nullSourceSuppliers);
        return parameterSuppliersFromTypedData(withNullCases);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new JsonExtract(source, args.get(0), args.get(1));
    }

    @Override
    public void testFold() {
        // FoldNull short-circuits the null-source case to a null literal before fold runs, so the
        // runtime warning never fires here. testEvaluate covers it via the evaluator path.
        if (isNullSourceCase(testCase)) {
            return;
        }
        super.testFold();
    }

    private static boolean isNullSourceCase(TestCaseSupplier.TestCase tc) {
        return tc.getData().size() > 0 && tc.getData().get(0).type() == DataType.SOURCE && tc.getData().get(0).getValue() == null;
    }

    private static TestCaseSupplier warningCase(String name, String json, String path, String warningMsg) {
        return warningCase(name, DataType.KEYWORD, DataType.KEYWORD, json, path, warningMsg);
    }

    private static TestCaseSupplier warningCase(
        String name,
        DataType jsonType,
        DataType pathType,
        String json,
        String path,
        String warningMsg
    ) {
        return new TestCaseSupplier(
            name,
            types(jsonType, pathType),
            () -> new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(json), jsonType, "str"),
                    new TestCaseSupplier.TypedData(new BytesRef(path), pathType, "path")
                ),
                "JsonExtractEvaluator[str=Attribute[channel=0], path=Attribute[channel=1]]",
                DataType.KEYWORD,
                nullValue()
            ).withWarning("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.")
                .withWarning("Line 1:1: java.lang.IllegalArgumentException: " + warningMsg)
        );
    }

    private static TestCaseSupplier foldingWarningCase(String name, String json, String path, String warningMsg) {
        return new TestCaseSupplier(
            name,
            types(DataType.KEYWORD, DataType.KEYWORD),
            () -> new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(json), DataType.KEYWORD, "str"),
                    new TestCaseSupplier.TypedData(new BytesRef(path), DataType.KEYWORD, "path")
                ),
                "JsonExtractEvaluator[str=Attribute[channel=0], path=Attribute[channel=1]]",
                DataType.KEYWORD,
                nullValue()
            ).withWarning("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.")
                .withWarning("Line 1:1: java.lang.IllegalArgumentException: " + warningMsg)
                .withFoldingException(IllegalArgumentException.class, warningMsg)
        );
    }

    private static List<DataType> types(DataType firstType, DataType secondType) {
        List<DataType> t = new ArrayList<>();
        t.add(firstType);
        t.add(secondType);
        return t;
    }
}
