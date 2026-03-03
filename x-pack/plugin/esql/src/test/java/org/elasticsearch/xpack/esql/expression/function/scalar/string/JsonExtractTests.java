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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

/**
 * Tests for {@link JsonExtract}. Parameterized suppliers cover type combinations, extraction
 * behavior (paths, bracket notation, unicode, warnings), and constant-path optimization through
 * the {@link AbstractScalarFunctionTestCase} framework. Standalone {@code testXxx()} methods cover
 * XContent encoding tests, randomized tests, evaluator type assertions, and programmatically
 * generated large/deep JSON tests.
 * <p>
 * Path parsing logic is tested separately in {@link JsonPathTests}.
 */
public class JsonExtractTests extends AbstractScalarFunctionTestCase {
    public JsonExtractTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        // --- Type combination coverage: all string type pairs ---
        for (DataType jsonType : DataType.stringTypes()) {
            for (DataType pathType : DataType.stringTypes()) {
                suppliers.add(
                    new TestCaseSupplier(
                        "extract string " + TestCaseSupplier.nameFromTypes(types(jsonType, pathType)),
                        types(jsonType, pathType),
                        () -> testCase(jsonType, pathType, "{\"name\":\"Alice\"}", "name", "Alice")
                    )
                );
            }
        }

        // --- SOURCE type support (for _source metadata field) ---
        for (DataType pathType : DataType.stringTypes()) {
            suppliers.add(
                new TestCaseSupplier(
                    "extract from source " + TestCaseSupplier.nameFromTypes(types(DataType.SOURCE, pathType)),
                    types(DataType.SOURCE, pathType),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(new BytesRef("{\"name\":\"Alice\"}"), DataType.SOURCE, "str"),
                            new TestCaseSupplier.TypedData(new BytesRef("name"), pathType, "path")
                        ),
                        expectedToString(),
                        DataType.KEYWORD,
                        equalTo(new BytesRef("Alice"))
                    )
                )
            );
        }

        // --- Constant path optimization: exercises JsonExtractConstantEvaluator via forceLiteral ---
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
                        "JsonExtractConstantEvaluator[str=Attribute[channel=0], path=name]",
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
                "JsonExtractConstantEvaluator[str=Attribute[channel=0], path=field]",
                DataType.KEYWORD,
                nullValue()
            ).withWarning("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.")
                .withWarning("Line 1:1: java.lang.IllegalArgumentException: invalid JSON input");
        }));

        // --- Named fixed cases: extraction behavior ---

        // Basic types
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

        // Object/array extraction (returned as JSON strings)
        suppliers.add(fixedCase("extract object as JSON string", "{\"obj\":{\"a\":1}}", "obj", "{\"a\":1}"));
        suppliers.add(fixedCase("extract array as JSON string", "{\"arr\":[1,2,3]}", "arr", "[1,2,3]"));
        suppliers.add(fixedCase("empty object", "{\"obj\":{}}", "obj", "{}"));
        suppliers.add(fixedCase("empty array", "{\"arr\":[]}", "arr", "[]"));
        suppliers.add(fixedCase("empty string value", "{\"val\":\"\"}", "val", ""));

        // Unicode
        suppliers.add(fixedCase("unicode key", "{\"名前\":\"太郎\"}", "名前", "太郎"));
        suppliers.add(fixedCase("unicode value", "{\"name\":\"Ñoño\"}", "name", "Ñoño"));
        suppliers.add(fixedCase("emoji key", "{\"🔑\":\"value\"}", "🔑", "value"));

        // Escaped characters in JSON values
        suppliers.add(fixedCase("quotes in value", "{\"msg\":\"she said \\\"hello\\\"\"}", "msg", "she said \"hello\""));
        suppliers.add(fixedCase("newline in value", "{\"msg\":\"line1\\nline2\"}", "msg", "line1\nline2"));
        suppliers.add(fixedCase("backslash in value", "{\"path\":\"C:\\\\Users\\\\test\"}", "path", "C:\\Users\\test"));

        // Duplicate keys
        suppliers.add(fixedCase("duplicate keys returns first", "{\"foo\":1,\"foo\":2}", "foo", "1"));

        // --- Null result cases (JSON null values, no warning) ---
        suppliers.add(nullResultCase("JSON null value", "{\"val\":null}", "val"));
        suppliers.add(nullResultCase("null in array", "{\"items\":[1,null,3]}", "items[1]"));
        suppliers.add(nullResultCase("bare $ returns null", "null", "$"));

        // --- Warning cases ---

        // Missing path / invalid input
        suppliers.add(warningCase("missing path", "{\"a\":1}", "b", "path [b] does not exist"));
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

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new JsonExtract(source, args.get(0), args.get(1));
    }

    // --- Evaluator type assertions ---

    public void testConstantPathProducesConstantEvaluator() {
        Source source = Source.synthetic("json_extract");
        var evaluatorFactory = evaluator(
            new JsonExtract(source, field("str", DataType.KEYWORD), new Literal(source, new BytesRef("name"), DataType.KEYWORD))
        );
        assertThat(evaluatorFactory, instanceOf(JsonExtractConstantEvaluator.Factory.class));
    }

    public void testNonConstantPathProducesGenericEvaluator() {
        Source source = Source.synthetic("json_extract");
        var evaluatorFactory = evaluator(new JsonExtract(source, field("str", DataType.KEYWORD), field("path", DataType.KEYWORD)));
        assertThat(evaluatorFactory, instanceOf(JsonExtractEvaluator.Factory.class));
    }

    // --- Large/deep JSON tests (programmatically generated) ---

    public void testDeeplyNestedPath() {
        // Build a 50-level deep nested JSON
        StringBuilder json = new StringBuilder();
        StringBuilder pathBuilder = new StringBuilder();
        for (int i = 0; i < 50; i++) {
            json.append("{\"k").append(i).append("\":");
            if (i > 0) {
                pathBuilder.append(".");
            }
            pathBuilder.append("k").append(i);
        }
        json.append("\"deep\"");
        for (int i = 0; i < 50; i++) {
            json.append("}");
        }
        assertResult(json.toString(), pathBuilder.toString(), "deep");
    }

    public void testLargeJsonArray() {
        // JSON array with 1000 elements, extract the last one
        StringBuilder json = new StringBuilder("{\"items\":[");
        for (int i = 0; i < 1000; i++) {
            if (i > 0) json.append(",");
            json.append(i);
        }
        json.append("]}");
        assertResult(json.toString(), "items[999]", "999");
    }

    public void testLargeJsonObject() {
        // JSON object with 1000 keys, extract the last one
        StringBuilder json = new StringBuilder("{");
        for (int i = 0; i < 1000; i++) {
            if (i > 0) json.append(",");
            json.append("\"key").append(i).append("\":").append(i);
        }
        json.append("}");
        assertResult(json.toString(), "key999", "999");
    }

    // --- XContent encoding tests ---
    // _source preserves the original encoding from indexing. These tests verify that
    // doExtract handles all Elasticsearch content types (JSON, SMILE, CBOR, YAML),
    // not just JSON. Each test runs across all four encodings.

    public void testAllEncodingsSimpleExtraction() throws IOException {
        forAllEncodings(Map.of("name", "Alice", "age", 30), "name", "Alice");
    }

    public void testAllEncodingsNestedExtraction() throws IOException {
        forAllEncodings(Map.of("user", Map.of("city", "London")), "user.city", "London");
    }

    public void testAllEncodingsArrayExtraction() throws IOException {
        forAllEncodings(Map.of("tags", List.of("a", "b", "c")), "tags[1]", "b");
    }

    public void testAllEncodingsNumericExtraction() throws IOException {
        forAllEncodings(Map.of("val", 42), "val", "42");
    }

    public void testAllEncodingsBooleanExtraction() throws IOException {
        forAllEncodings(Map.of("flag", true), "flag", "true");
    }

    public void testAllEncodingsFloatingPointExtraction() throws IOException {
        forAllEncodings(Map.of("val", 3.14159), "val", "3.14159");
    }

    public void testAllEncodingsNegativeNumberExtraction() throws IOException {
        forAllEncodings(Map.of("val", -42), "val", "-42");
    }

    public void testAllEncodingsEmptyStringExtraction() throws IOException {
        forAllEncodings(Map.of("val", ""), "val", "");
    }

    public void testAllEncodingsUnicodeExtraction() throws IOException {
        forAllEncodings(Map.of("name", "Ñoño"), "name", "Ñoño");
    }

    public void testAllEncodingsDeepNesting() throws IOException {
        forAllEncodings(Map.of("a", Map.of("b", Map.of("c", Map.of("d", "deep")))), "a.b.c.d", "deep");
    }

    public void testAllEncodingsMixedArrayAndObject() throws IOException {
        forAllEncodings(
            Map.of("orders", List.of(Map.of("id", 1, "item", "book"), Map.of("id", 2, "item", "pen"))),
            "orders[1].item",
            "pen"
        );
    }

    // --- Randomized tests across all XContent encodings ---

    /**
     * Generates a random flat object, picks a random key, and verifies extraction
     * across all four XContent encodings.
     */
    public void testRandomFlatObjectAllEncodings() throws IOException {
        int numKeys = randomIntBetween(1, 20);
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < numKeys; i++) {
            map.put("key" + i, randomAlphaOfLengthBetween(1, 50));
        }
        String targetKey = "key" + randomIntBetween(0, numKeys - 1);
        String expected = (String) map.get(targetKey);
        forAllEncodings(map, targetKey, expected);
    }

    /**
     * Generates a random nested object (2-4 levels deep) with sibling noise keys,
     * builds a dot-notation path to a leaf value, and verifies extraction across
     * all four XContent encodings.
     */
    public void testRandomNestedObjectAllEncodings() throws IOException {
        int depth = randomIntBetween(2, 4);
        String leafValue = randomAlphaOfLengthBetween(1, 30);

        Map<String, Object> innermost = new HashMap<>();
        String leafKey = "leaf" + randomIntBetween(0, 99);
        innermost.put(leafKey, leafValue);
        for (int i = 0; i < randomIntBetween(0, 3); i++) {
            innermost.put("sibling" + i, randomAlphaOfLengthBetween(1, 10));
        }

        Object nested = innermost;
        List<String> pathParts = new ArrayList<>();
        pathParts.add(leafKey);
        for (int i = depth - 1; i >= 0; i--) {
            String key = "level" + i;
            Map<String, Object> wrapper = new HashMap<>();
            wrapper.put(key, nested);
            for (int j = 0; j < randomIntBetween(0, 3); j++) {
                wrapper.put("noise" + j, randomAlphaOfLengthBetween(1, 10));
            }
            nested = wrapper;
            pathParts.add(key);
        }
        Collections.reverse(pathParts);
        String path = String.join(".", pathParts);

        @SuppressWarnings("unchecked")
        Map<String, Object> root = (Map<String, Object>) nested;
        forAllEncodings(root, path, leafValue);
    }

    /**
     * Generates a random array, picks a random index, and verifies extraction
     * across all four XContent encodings.
     */
    public void testRandomArrayIndexAllEncodings() throws IOException {
        int arraySize = randomIntBetween(1, 20);
        List<String> items = new ArrayList<>();
        for (int i = 0; i < arraySize; i++) {
            items.add(randomAlphaOfLengthBetween(1, 20));
        }
        int targetIndex = randomIntBetween(0, arraySize - 1);
        forAllEncodings(Map.of("items", items), "items[" + targetIndex + "]", items.get(targetIndex));
    }

    /**
     * Generates random scalar values (string, integer, boolean) and verifies
     * extraction of each type across all four XContent encodings.
     */
    public void testRandomScalarTypesAllEncodings() throws IOException {
        String strVal = randomAlphaOfLengthBetween(1, 30);
        int intVal = randomIntBetween(-10000, 10000);
        boolean boolVal = randomBoolean();

        Map<String, Object> map = new LinkedHashMap<>();
        map.put("str", strVal);
        map.put("num", intVal);
        map.put("flag", boolVal);

        forAllEncodings(map, "str", strVal);
        forAllEncodings(map, "num", Integer.toString(intVal));
        forAllEncodings(map, "flag", Boolean.toString(boolVal));
    }

    // --- Parameterized test case helpers ---

    private static TestCaseSupplier.TestCase testCase(DataType jsonType, DataType pathType, String json, String path, String expected) {
        return new TestCaseSupplier.TestCase(
            List.of(
                new TestCaseSupplier.TypedData(new BytesRef(json), jsonType, "str"),
                new TestCaseSupplier.TypedData(new BytesRef(path), pathType, "path")
            ),
            expectedToString(),
            DataType.KEYWORD,
            equalTo(new BytesRef(expected))
        );
    }

    private static TestCaseSupplier fixedCase(String name, String json, String path, String expected) {
        return new TestCaseSupplier(
            name,
            types(DataType.KEYWORD, DataType.KEYWORD),
            () -> testCase(DataType.KEYWORD, DataType.KEYWORD, json, path, expected)
        );
    }

    private static TestCaseSupplier warningCase(String name, String json, String path, String warningMsg) {
        return new TestCaseSupplier(
            name,
            types(DataType.KEYWORD, DataType.KEYWORD),
            () -> new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(json), DataType.KEYWORD, "str"),
                    new TestCaseSupplier.TypedData(new BytesRef(path), DataType.KEYWORD, "path")
                ),
                expectedToString(),
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
                expectedToString(),
                DataType.KEYWORD,
                nullValue()
            ).withWarning("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.")
                .withWarning("Line 1:1: java.lang.IllegalArgumentException: " + warningMsg)
                .withFoldingException(IllegalArgumentException.class, warningMsg)
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
                expectedToString(),
                DataType.KEYWORD,
                nullValue()
            )
        );
    }

    // --- XContent encoding helpers ---

    private static final List<XContentType> ALL_XCONTENT_TYPES = List.of(
        XContentType.JSON,
        XContentType.SMILE,
        XContentType.CBOR,
        XContentType.YAML
    );

    private void forAllEncodings(Map<String, ?> map, String path, String expected) throws IOException {
        for (XContentType type : ALL_XCONTENT_TYPES) {
            BytesRef bytes = encodeAsXContent(map, type);
            String result = extractFromBytes(bytes, path);
            assertThat("[" + type + "] path " + path, result, equalTo(expected));
        }
    }

    private static BytesRef encodeAsXContent(Map<String, ?> map, XContentType type) throws IOException {
        try (XContentBuilder builder = XContentBuilder.builder(type.xContent())) {
            builder.map(map);
            return BytesReference.bytes(builder).toBytesRef();
        }
    }

    // --- Standalone test helpers ---

    private String extractFromBytes(BytesRef bytes, String path) {
        try (
            var eval = evaluator(new JsonExtract(Source.EMPTY, field("str", DataType.KEYWORD), field("path", DataType.KEYWORD))).get(
                driverContext()
            );
            Block block = eval.eval(row(List.of(bytes, new BytesRef(path))))
        ) {
            return block.isNull(0) ? null : ((BytesRef) BlockUtils.toJavaObject(block, 0)).utf8ToString();
        }
    }

    private void assertResult(String json, String path, String expected) {
        assertThat(extractFromBytes(new BytesRef(json), path), equalTo(expected));
    }

    // --- Utility helpers ---

    private static String expectedToString() {
        return "JsonExtractEvaluator[str=Attribute[channel=0], path=Attribute[channel=1]]";
    }

    private static List<DataType> types(DataType firstType, DataType secondType) {
        List<DataType> types = new ArrayList<>();
        types.add(firstType);
        types.add(secondType);
        return types;
    }
}
