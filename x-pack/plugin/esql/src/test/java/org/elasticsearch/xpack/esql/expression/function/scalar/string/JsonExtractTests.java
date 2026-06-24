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
import static org.hamcrest.Matchers.startsWith;

/**
 * Core extraction behavior for {@link JsonExtract}: type-combination coverage, constant-path
 * optimization, basic value extraction (string/number/boolean/scalar), nested-object and
 * array-index navigation, JSON-string object/array extraction, escaped characters in values,
 * duplicate keys, and {@code null}-JSON-value handling.
 * <p>
 * Companion classes split the rest of the surface:
 * <ul>
 *   <li>{@link JsonExtractPathSyntaxTests} — bracket notation, {@code $} prefix, Unicode keys.</li>
 *   <li>{@link JsonExtractWarningTests} — warning-emitting cases (invalid JSON, missing path,
 *       array bounds, unsupported JSONPath syntax, null-source).</li>
 *   <li>{@link JsonExtractStaticTests} — large/deep input, all-encoding coverage, randomized
 *       fuzz, evaluator-type assertions.</li>
 * </ul>
 * The split keeps each class's parameterized descriptor count under the 512m default test heap
 * when the {@code repeat-changed-tests} runner expands every test method at
 * {@code -Dtests.iters=100}. Path parsing is tested separately in {@link JsonPathTests}.
 */
public class JsonExtractTests extends AbstractScalarFunctionTestCase {

    public JsonExtractTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        // Type-coverage: every declared str/path type pair. SOURCE-typed suppliers use the
        // null-source variant — its declared warning makes the framework's null-injection
        // tests skip (via assumeTrue("no warning is expected", ...)) while still exercising
        // the SOURCE evaluator. Happy-path SOURCE extraction is in JsonExtractStaticTests.
        for (DataType jsonType : DataType.stringTypes()) {
            for (DataType pathType : DataType.stringTypes()) {
                suppliers.add(
                    typedCase(
                        "extract " + jsonType.esNameIfPossible() + "/" + pathType.esNameIfPossible(),
                        jsonType,
                        pathType,
                        "{\"name\":\"Alice\"}",
                        "name",
                        "Alice"
                    )
                );
            }
        }
        List<TestCaseSupplier> sourceSuppliers = new ArrayList<>();
        for (DataType pathType : DataType.stringTypes()) {
            sourceSuppliers.add(nullSourceCase("null source " + pathType.esNameIfPossible(), pathType));
        }

        // Constant path optimization — exercises JsonExtractConstantEvaluator via forceLiteral.
        for (DataType jsonType : DataType.stringTypes()) {
            suppliers.add(
                new TestCaseSupplier(
                    "constant path " + TestCaseSupplier.nameFromTypes(types(jsonType, DataType.KEYWORD)),
                    types(jsonType, DataType.KEYWORD),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(new BytesRef("{\"name\":\"Alice\"}"), jsonType, "str"),
                            new TestCaseSupplier.TypedData(new BytesRef("name"), DataType.KEYWORD, "path").forceLiteral()
                        ),
                        startsWith("JsonExtractConstantEvaluator[str=Attribute[channel=0], path=name]"),
                        DataType.KEYWORD,
                        equalTo(new BytesRef("Alice"))
                    )
                )
            );
        }

        // Constant path warning case
        suppliers.add(new TestCaseSupplier("constant invalid json", types(DataType.KEYWORD, DataType.KEYWORD), () -> {
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef("not valid json"), DataType.KEYWORD, "str"),
                    new TestCaseSupplier.TypedData(new BytesRef("field"), DataType.KEYWORD, "path").forceLiteral()
                ),
                startsWith("JsonExtractConstantEvaluator[str=Attribute[channel=0], path=field]"),
                DataType.KEYWORD,
                nullValue()
            ).withWarning("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.")
                .withWarning("Line 1:1: java.lang.IllegalArgumentException: invalid JSON input");
        }));

        // --- Named fixed cases: extraction behavior ---

        // Basic value types
        suppliers.add(fixedCase("string value", "{\"name\":\"Alice\"}", "name", "Alice"));
        suppliers.add(fixedCase("number value", "{\"val\":42}", "val", "42"));
        suppliers.add(fixedCase("boolean value", "{\"flag\":true}", "flag", "true"));
        suppliers.add(fixedCase("floating point", "{\"val\":3.14159}", "val", "3.14159"));
        suppliers.add(fixedCase("scientific notation", "{\"val\":1.5E10}", "val", "1.5E10"));
        suppliers.add(fixedCase("negative number", "{\"val\":-42}", "val", "-42"));

        // Nested paths
        suppliers.add(fixedCase("dot notation nested", "{\"user\":{\"city\":\"London\"}}", "user.city", "London"));
        suppliers.add(fixedCase("deep nesting", "{\"a\":{\"b\":{\"c\":{\"d\":\"deep\"}}}}", "a.b.c.d", "deep"));

        // Array access
        suppliers.add(fixedCase("array index", "{\"tags\":[\"a\",\"b\"]}", "tags[0]", "a"));
        suppliers.add(fixedCase("mixed array and object nesting", "{\"orders\":[{\"id\":1},{\"id\":2}]}", "orders[1].id", "2"));
        suppliers.add(
            fixedCase(
                "deep mixed nesting",
                "{\"company\":{\"departments\":[{\"name\":\"eng\",\"leads\":[{\"name\":\"Alice\"},{\"name\":\"Bob\"}]}]}}",
                "company.departments[0].leads[1].name",
                "Bob"
            )
        );

        // Object/array extraction (returned as JSON strings)
        suppliers.add(fixedCase("extract object as JSON string", "{\"obj\":{\"a\":1}}", "obj", "{\"a\":1}"));
        suppliers.add(fixedCase("extract array as JSON string", "{\"arr\":[1,2,3]}", "arr", "[1,2,3]"));
        suppliers.add(fixedCase("empty object", "{\"obj\":{}}", "obj", "{}"));
        suppliers.add(fixedCase("empty array", "{\"arr\":[]}", "arr", "[]"));
        suppliers.add(fixedCase("empty string value", "{\"val\":\"\"}", "val", ""));

        // Escaped characters in JSON values
        suppliers.add(fixedCase("quotes in value", "{\"msg\":\"she said \\\"hello\\\"\"}", "msg", "she said \"hello\""));
        suppliers.add(fixedCase("newline in value", "{\"msg\":\"line1\\nline2\"}", "msg", "line1\nline2"));
        suppliers.add(fixedCase("backslash in value", "{\"path\":\"C:\\\\Users\\\\test\"}", "path", "C:\\Users\\test"));

        // Duplicate keys
        suppliers.add(fixedCase("duplicate keys returns first", "{\"foo\":1,\"foo\":2}", "foo", "1"));

        // JSON null result cases (no warning)
        suppliers.add(nullResultCase("JSON null value", "{\"val\":null}", "val"));
        suppliers.add(nullResultCase("null in array", "{\"items\":[1,null,3]}", "items[1]"));
        suppliers.add(nullResultCase("bare $ returns null", "null", "$"));

        List<TestCaseSupplier> withNullCases = anyNullIsNull(true, suppliers);
        withNullCases.addAll(sourceSuppliers);
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

    private static TestCaseSupplier fixedCase(String name, String json, String path, String expected) {
        return typedCase(name, DataType.KEYWORD, DataType.KEYWORD, json, path, expected);
    }

    private static TestCaseSupplier typedCase(
        String name,
        DataType jsonType,
        DataType pathType,
        String json,
        String path,
        String expected
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
                equalTo(new BytesRef(expected))
            )
        );
    }

    /**
     * SOURCE-typed null-source supplier. Same shape as {@code JsonExtractWarningTests}'s null-source
     * supplier — included here purely to give this class non-empty {@code _source} coverage so the
     * {@code @AfterClass testFunctionInfo} check passes. The declared warning makes the framework's
     * null-injection tests skip via {@code assumeTrue("no warning is expected", ...)}.
     */
    private static TestCaseSupplier nullSourceCase(String name, DataType pathType) {
        return new TestCaseSupplier(
            name,
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
        );
    }

    private static TestCaseSupplier nullResultCase(String name, String json, String path) {
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
            )
        );
    }

    private static List<DataType> types(DataType firstType, DataType secondType) {
        List<DataType> t = new ArrayList<>();
        t.add(firstType);
        t.add(secondType);
        return t;
    }
}
