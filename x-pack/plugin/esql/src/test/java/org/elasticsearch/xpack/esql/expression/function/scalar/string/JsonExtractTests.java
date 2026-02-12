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

/**
 * Tests for {@link JsonExtract} function.
 */
public class JsonExtractTests extends AbstractScalarFunctionTestCase {
    public JsonExtractTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        // Randomized test across string type combinations
        for (DataType jsonType : DataType.stringTypes()) {
            for (DataType pathType : DataType.stringTypes()) {
                suppliers.add(
                    new TestCaseSupplier(
                        "extract string " + TestCaseSupplier.nameFromTypes(types(jsonType, pathType)),
                        types(jsonType, pathType),
                        () -> new TestCaseSupplier.TestCase(
                            List.of(
                                new TestCaseSupplier.TypedData(new BytesRef("{\"name\":\"Alice\"}"), jsonType, "jsonInput"),
                                new TestCaseSupplier.TypedData(new BytesRef("name"), pathType, "path")
                            ),
                            expectedToString(),
                            DataType.KEYWORD,
                            equalTo(new BytesRef("Alice"))
                        )
                    )
                );
            }
        }

        // String value extraction
        suppliers.add(supplier("{\"name\":\"Alice\",\"age\":30}", "name", new BytesRef("Alice")));

        // Number extraction (returned as keyword string)
        suppliers.add(supplier("{\"name\":\"Alice\",\"age\":30}", "age", new BytesRef("30")));

        // Boolean extraction
        suppliers.add(supplier("{\"active\":true}", "active", new BytesRef("true")));
        suppliers.add(supplier("{\"active\":false}", "active", new BytesRef("false")));

        // Nested field extraction
        suppliers.add(supplier("{\"user\":{\"address\":{\"city\":\"London\"}}}", "user.address.city", new BytesRef("London")));

        // Array index extraction
        suppliers.add(supplier("{\"tags\":[\"a\",\"b\",\"c\"]}", "tags[0]", new BytesRef("a")));
        suppliers.add(supplier("{\"tags\":[\"a\",\"b\",\"c\"]}", "tags[2]", new BytesRef("c")));

        // Mixed nesting
        suppliers.add(
            supplier("{\"orders\":[{\"id\":1,\"item\":\"book\"},{\"id\":2,\"item\":\"pen\"}]}", "orders[1].item", new BytesRef("pen"))
        );

        // Missing path returns null with warning
        suppliers.add(new TestCaseSupplier("missing path", types(DataType.KEYWORD, DataType.KEYWORD), () -> {
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef("{\"name\":\"Alice\"}"), DataType.KEYWORD, "jsonInput"),
                    new TestCaseSupplier.TypedData(new BytesRef("nonexistent"), DataType.KEYWORD, "path")
                ),
                expectedToString(),
                DataType.KEYWORD,
                nullValue()
            ).withWarning("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.")
                .withWarning("Line 1:1: java.lang.IllegalArgumentException: path [nonexistent] does not exist");
        }));

        // JSON null returns ES|QL null (no warning)
        suppliers.add(supplier("{\"value\":null}", "value", null));

        // Array out of bounds returns null with warning
        suppliers.add(new TestCaseSupplier("array out of bounds", types(DataType.KEYWORD, DataType.KEYWORD), () -> {
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef("{\"tags\":[\"a\",\"b\"]}"), DataType.KEYWORD, "jsonInput"),
                    new TestCaseSupplier.TypedData(new BytesRef("tags[5]"), DataType.KEYWORD, "path")
                ),
                expectedToString(),
                DataType.KEYWORD,
                nullValue()
            ).withWarning("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.")
                .withWarning("Line 1:1: java.lang.IllegalArgumentException: array index out of bounds");
        }));

        // Traversal through non-object returns null with warning
        suppliers.add(new TestCaseSupplier("non-object traversal", types(DataType.KEYWORD, DataType.KEYWORD), () -> {
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef("{\"name\":\"Alice\"}"), DataType.KEYWORD, "jsonInput"),
                    new TestCaseSupplier.TypedData(new BytesRef("name.nested"), DataType.KEYWORD, "path")
                ),
                expectedToString(),
                DataType.KEYWORD,
                nullValue()
            ).withWarning("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.")
                .withWarning("Line 1:1: java.lang.IllegalArgumentException: path [name.nested] does not exist");
        }));

        // Extract nested object as JSON string
        suppliers.add(supplier("{\"user\":{\"name\":\"Alice\",\"age\":30}}", "user", new BytesRef("{\"name\":\"Alice\",\"age\":30}")));

        // Extract array as JSON string
        suppliers.add(supplier("{\"tags\":[\"a\",\"b\",\"c\"]}", "tags", new BytesRef("[\"a\",\"b\",\"c\"]")));

        // Invalid JSON - warning case
        suppliers.add(new TestCaseSupplier("invalid json", types(DataType.KEYWORD, DataType.KEYWORD), () -> {
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef("not valid json"), DataType.KEYWORD, "jsonInput"),
                    new TestCaseSupplier.TypedData(new BytesRef("field"), DataType.KEYWORD, "path")
                ),
                expectedToString(),
                DataType.KEYWORD,
                nullValue()
            ).withWarning("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.")
                .withWarning("Line 1:1: java.lang.IllegalArgumentException: invalid JSON input");
        }));

        // SOURCE type support (for _source field)
        for (DataType pathType : DataType.stringTypes()) {
            suppliers.add(
                new TestCaseSupplier(
                    "extract from source " + TestCaseSupplier.nameFromTypes(types(DataType.SOURCE, pathType)),
                    types(DataType.SOURCE, pathType),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(new BytesRef("{\"name\":\"Alice\"}"), DataType.SOURCE, "jsonInput"),
                            new TestCaseSupplier.TypedData(new BytesRef("name"), pathType, "path")
                        ),
                        expectedToString(),
                        DataType.KEYWORD,
                        equalTo(new BytesRef("Alice"))
                    )
                )
            );
        }

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new JsonExtract(source, args.get(0), args.get(1));
    }

    private static TestCaseSupplier supplier(String json, String path, BytesRef expectedValue) {
        String name = String.format("extract \"%s\" from json", path);
        return new TestCaseSupplier(name, types(DataType.KEYWORD, DataType.KEYWORD), () -> {
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(json), DataType.KEYWORD, "jsonInput"),
                    new TestCaseSupplier.TypedData(new BytesRef(path), DataType.KEYWORD, "path")
                ),
                expectedToString(),
                DataType.KEYWORD,
                expectedValue == null ? nullValue() : equalTo(expectedValue)
            );
        });
    }

    private static String expectedToString() {
        return "JsonExtractEvaluator[jsonInput=Attribute[channel=0], path=Attribute[channel=1]]";
    }

    private static List<DataType> types(DataType firstType, DataType secondType) {
        List<DataType> types = new ArrayList<>();
        types.add(firstType);
        types.add(secondType);
        return types;
    }
}
