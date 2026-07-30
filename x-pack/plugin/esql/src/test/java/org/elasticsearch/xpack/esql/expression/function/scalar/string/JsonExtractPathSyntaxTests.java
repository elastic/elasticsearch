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

/**
 * JSONPath syntax variants for {@link JsonExtract}: bracket notation (with quoted keys, dots,
 * spaces, escaped chars), the {@code $} root prefix, bare {@code $} variants, root access edge
 * cases, and Unicode keys/values. Split off {@link JsonExtractTests} so the
 * {@code repeat-changed-tests} runner stays under 512m heap.
 */
public class JsonExtractPathSyntaxTests extends AbstractScalarFunctionTestCase {

    public JsonExtractPathSyntaxTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        // Type-coverage suppliers — every declared str/path type pair gets a basic bracket case so
        // the @AfterClass testFunctionInfo check sees the full set this function declares.
        for (DataType jsonType : DataType.stringTypes()) {
            for (DataType pathType : DataType.stringTypes()) {
                suppliers.add(
                    typedCase(
                        "bracket " + jsonType.esNameIfPossible() + "/" + pathType.esNameIfPossible(),
                        jsonType,
                        pathType,
                        "{\"name\":\"Alice\"}",
                        "['name']",
                        "Alice"
                    )
                );
            }
        }
        // SOURCE-typed null-source supplier — the declared warning makes the framework's
        // null-injection tests skip while still exercising the SOURCE evaluator routing.
        // Happy-path SOURCE extraction is in JsonExtractStaticTests.
        List<TestCaseSupplier> sourceSuppliers = new ArrayList<>();
        for (DataType pathType : DataType.stringTypes()) {
            sourceSuppliers.add(nullSourceCase("null source " + pathType.esNameIfPossible(), pathType));
        }

        // Bracket notation
        suppliers.add(fixedCase("bracket single-quoted key", "{\"name\":\"Alice\"}", "['name']", "Alice"));
        suppliers.add(fixedCase("bracket double-quoted key", "{\"name\":\"Alice\"}", "[\"name\"]", "Alice"));
        suppliers.add(fixedCase("bracket key with dot", "{\"user.name\":\"Alice\"}", "['user.name']", "Alice"));
        suppliers.add(fixedCase("bracket key with space", "{\"first name\":\"Bob\"}", "['first name']", "Bob"));
        suppliers.add(fixedCase("bracket key with brackets", "{\"items[0]\":\"value\"}", "['items[0]']", "value"));
        suppliers.add(fixedCase("bracket nested", "{\"a\":{\"b.c\":42}}", "a['b.c']", "42"));
        suppliers.add(
            fixedCase("mixed dot and bracket", "{\"store\":{\"user.name\":\"Alice\",\"city\":\"London\"}}", "store['user.name']", "Alice")
        );
        suppliers.add(fixedCase("consecutive brackets", "{\"a\":{\"b.c\":{\"d\":1}}}", "a['b.c']['d']", "1"));
        suppliers.add(fixedCase("bracket after array index", "{\"arr\":[{\"a.b\":1},{\"a.b\":2}]}", "arr[0]['a.b']", "1"));
        suppliers.add(fixedCase("escaped single quote in key", "{\"it's\":\"value\"}", "['it\\'s']", "value"));
        suppliers.add(fixedCase("escaped double quote in key", "{\"say \\\"hi\\\"\":\"value\"}", "[\"say \\\"hi\\\"\"]", "value"));
        suppliers.add(fixedCase("escaped backslash in key", "{\"a\\\\b\":\"value\"}", "['a\\\\b']", "value"));
        suppliers.add(fixedCase("empty string key", "{\"\":\"value\"}", "['']", "value"));

        // Dollar prefix
        suppliers.add(fixedCase("$.name", "{\"name\":\"Alice\"}", "$.name", "Alice"));
        suppliers.add(fixedCase("$.nested path", "{\"user\":{\"address\":{\"city\":\"London\"}}}", "$.user.address.city", "London"));
        suppliers.add(fixedCase("$.array index", "{\"tags\":[\"a\",\"b\"]}", "$.tags[0]", "a"));
        suppliers.add(fixedCase("$.mixed nesting", "{\"orders\":[{\"id\":1},{\"id\":2}]}", "$.orders[1].id", "2"));
        suppliers.add(fixedCase("$['bracket']", "{\"user.name\":\"Alice\"}", "$['user.name']", "Alice"));
        suppliers.add(fixedCase("$[0] array index", "[10,20,30]", "$[1]", "20"));
        suppliers.add(fixedCase("$['a'].b nested", "{\"a\":{\"b\":1}}", "$['a'].b", "1"));

        // Bare $ with different JSON value types
        suppliers.add(fixedCase("bare $ returns object", "{\"a\":1,\"b\":2}", "$", "{\"a\":1,\"b\":2}"));
        suppliers.add(fixedCase("bare $ returns array", "[1,2,3]", "$", "[1,2,3]"));
        suppliers.add(fixedCase("bare $ returns string", "\"hello\"", "$", "hello"));
        suppliers.add(fixedCase("bare $ returns number", "42", "$", "42"));
        suppliers.add(fixedCase("bare $ returns boolean", "true", "$", "true"));

        // Root access
        suppliers.add(fixedCase("empty path returns root", "{\"a\":1}", "", "{\"a\":1}"));
        suppliers.add(fixedCase("$. returns root", "{\"a\":1}", "$.", "{\"a\":1}"));
        suppliers.add(fixedCase("bare [0] array index", "[1,2,3]", "[0]", "1"));

        // Unicode
        suppliers.add(fixedCase("unicode key", "{\"名前\":\"太郎\"}", "名前", "太郎"));
        suppliers.add(fixedCase("unicode value", "{\"name\":\"Ñoño\"}", "name", "Ñoño"));
        suppliers.add(fixedCase("emoji key", "{\"🔑\":\"value\"}", "🔑", "value"));

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

    private static TestCaseSupplier nullSourceCase(String name, DataType pathType) {
        return new TestCaseSupplier(
            name,
            types(DataType.SOURCE, pathType),
            () -> new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(null, DataType.SOURCE, "str"),
                    new TestCaseSupplier.TypedData(new BytesRef("['name']"), pathType, "path")
                ),
                "JsonExtractSourceEvaluator[strBlock=Attribute[channel=0], pathBlock=Attribute[channel=1]]",
                org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD,
                org.hamcrest.Matchers.nullValue()
            ).withWarning("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.")
                .withWarning(
                    "Line 1:1: java.lang.IllegalStateException: "
                        + "_source is null; this typically means the index has _source disabled in its mapping"
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
