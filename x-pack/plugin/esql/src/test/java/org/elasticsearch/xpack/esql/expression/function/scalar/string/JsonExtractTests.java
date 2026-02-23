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
 * Tests for {@link JsonExtract}. Parameterized suppliers cover type combinations, warning
 * propagation, and constant-path optimization through the {@link AbstractScalarFunctionTestCase}
 * framework. Inline {@code testXxx()} methods cover deterministic extraction scenarios
 * (path syntax, edge cases, XContent encodings) and randomized tests.
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

        // Type combination coverage: all string type pairs
        for (DataType jsonType : DataType.stringTypes()) {
            for (DataType pathType : DataType.stringTypes()) {
                suppliers.add(
                    new TestCaseSupplier(
                        "extract string " + TestCaseSupplier.nameFromTypes(types(jsonType, pathType)),
                        types(jsonType, pathType),
                        () -> new TestCaseSupplier.TestCase(
                            List.of(
                                new TestCaseSupplier.TypedData(new BytesRef("{\"name\":\"Alice\"}"), jsonType, "str"),
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

        // Warning case: invalid JSON (exercises warning propagation through the framework)
        suppliers.add(new TestCaseSupplier("invalid json", types(DataType.KEYWORD, DataType.KEYWORD), () -> {
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef("not valid json"), DataType.KEYWORD, "str"),
                    new TestCaseSupplier.TypedData(new BytesRef("field"), DataType.KEYWORD, "path")
                ),
                expectedToString(),
                DataType.KEYWORD,
                nullValue()
            ).withWarning("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.")
                .withWarning("Line 1:1: java.lang.IllegalArgumentException: invalid JSON input");
        }));

        // Constant path optimization: exercises the JsonExtractConstantEvaluator via forceLiteral
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

        // SOURCE type support (for _source metadata field)
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

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new JsonExtract(source, args.get(0), args.get(1));
    }

    // --- Root accessor ---

    public void testEmptyPathReturnsRoot() {
        assertResult("{\"a\":1}", "", "{\"a\":1}");
    }

    // --- JSONPath $ prefix ---

    public void testDollarDotPrefix() {
        assertResult("{\"name\":\"Alice\"}", "$.name", "Alice");
    }

    public void testDollarDotNestedPath() {
        assertResult("{\"user\":{\"address\":{\"city\":\"London\"}}}", "$.user.address.city", "London");
    }

    public void testDollarDotArrayIndex() {
        assertResult("{\"tags\":[\"a\",\"b\"]}", "$.tags[0]", "a");
    }

    public void testDollarDotMixedNesting() {
        assertResult("{\"orders\":[{\"id\":1},{\"id\":2}]}", "$.orders[1].id", "2");
    }

    public void testDeepMixedNesting() {
        assertResult(
            "{\"company\":{\"departments\":[{\"name\":\"eng\",\"leads\":[{\"name\":\"Alice\"},{\"name\":\"Bob\"}]}]}}",
            "company.departments[0].leads[1].name",
            "Bob"
        );
    }

    public void testDollarDotAloneReturnsRoot() {
        assertResult("{\"a\":1}", "$.", "{\"a\":1}");
    }

    // --- Bare $ root accessor ---

    public void testBareDollarReturnsObject() {
        assertResult("{\"a\":1,\"b\":2}", "$", "{\"a\":1,\"b\":2}");
    }

    public void testBareDollarReturnsArray() {
        assertResult("[1,2,3]", "$", "[1,2,3]");
    }

    public void testBareDollarReturnsString() {
        assertResult("\"hello\"", "$", "hello");
    }

    public void testBareDollarReturnsNumber() {
        assertResult("42", "$", "42");
    }

    public void testBareDollarReturnsBoolean() {
        assertResult("true", "$", "true");
    }

    public void testBareDollarReturnsNull() {
        assertNullResult("null", "$");
    }

    // --- Bracket notation for named keys ---

    public void testSingleQuotedKey() {
        assertResult("{\"name\":\"Alice\"}", "['name']", "Alice");
    }

    public void testDoubleQuotedKey() {
        assertResult("{\"name\":\"Alice\"}", "[\"name\"]", "Alice");
    }

    public void testQuotedKeyWithDot() {
        assertResult("{\"user.name\":\"Alice\"}", "['user.name']", "Alice");
    }

    public void testQuotedKeyWithSpace() {
        assertResult("{\"first name\":\"Bob\"}", "['first name']", "Bob");
    }

    public void testQuotedKeyWithBrackets() {
        assertResult("{\"items[0]\":\"value\"}", "['items[0]']", "value");
    }

    public void testQuotedKeyNested() {
        assertResult("{\"a\":{\"b.c\":42}}", "a['b.c']", "42");
    }

    public void testMixedDotAndBracketNotation() {
        assertResult("{\"store\":{\"user.name\":\"Alice\",\"city\":\"London\"}}", "store['user.name']", "Alice");
    }

    public void testConsecutiveBracketNotation() {
        assertResult("{\"a\":{\"b.c\":{\"d\":1}}}", "a['b.c']['d']", "1");
    }

    public void testBracketNotationAfterArrayIndex() {
        assertResult("{\"arr\":[{\"a.b\":1},{\"a.b\":2}]}", "arr[0]['a.b']", "1");
    }

    public void testEscapedSingleQuoteInKey() {
        assertResult("{\"it's\":\"value\"}", "['it\\'s']", "value");
    }

    public void testEscapedDoubleQuoteInKey() {
        assertResult("{\"say \\\"hi\\\"\":\"value\"}", "[\"say \\\"hi\\\"\"]", "value");
    }

    public void testEscapedBackslashInKey() {
        assertResult("{\"a\\\\b\":\"value\"}", "['a\\\\b']", "value");
    }

    public void testDollarPrefixWithBracketNotation() {
        assertResult("{\"user.name\":\"Alice\"}", "$['user.name']", "Alice");
    }

    public void testDollarBracketArrayIndex() {
        assertResult("[10,20,30]", "$[1]", "20");
    }

    public void testBareLeadingBracketArrayIndex() {
        assertResult("[1,2,3]", "[0]", "1");
    }

    public void testDollarBracketNestedPath() {
        assertResult("{\"a\":{\"b\":1}}", "$['a'].b", "1");
    }

    public void testEmptyStringKey() {
        assertResult("{\"\":\"value\"}", "['']", "value");
    }

    // --- Unicode handling ---

    public void testUnicodeKey() {
        assertResult("{\"名前\":\"太郎\"}", "名前", "太郎");
    }

    public void testUnicodeValue() {
        assertResult("{\"name\":\"Ñoño\"}", "name", "Ñoño");
    }

    public void testEmojiKey() {
        assertResult("{\"🔑\":\"value\"}", "🔑", "value");
    }

    // --- Escaped characters in JSON ---

    public void testEscapedQuotesInValue() {
        assertResult("{\"msg\":\"she said \\\"hello\\\"\"}", "msg", "she said \"hello\"");
    }

    public void testNewlineInValue() {
        assertResult("{\"msg\":\"line1\\nline2\"}", "msg", "line1\nline2");
    }

    public void testBackslashInValue() {
        assertResult("{\"path\":\"C:\\\\Users\\\\test\"}", "path", "C:\\Users\\test");
    }

    // --- Deeply nested JSON ---

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

    // --- Large JSON ---

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

    // --- Constant path specialization ---

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

    // --- Numeric edge cases ---

    public void testNegativeArrayIndex() {
        assertWarningResult("{\"a\":[1,2,3]}", "a[-1]", "array index out of bounds");
    }

    public void testFloatingPointNumber() {
        assertResult("{\"val\":3.14159}", "val", "3.14159");
    }

    public void testScientificNotation() {
        assertResult("{\"val\":1.5E10}", "val", "1.5E10");
    }

    public void testNegativeNumber() {
        assertResult("{\"val\":-42}", "val", "-42");
    }

    // --- Duplicate keys and null-in-array ---

    public void testDuplicateKeysReturnsFirstMatch() {
        assertResult("{\"foo\":1,\"foo\":2}", "foo", "1");
    }

    public void testNullInsideArray() {
        assertNullResult("{\"items\":[1,null,3]}", "items[1]");
    }

    // --- Empty structures ---

    public void testEmptyObject() {
        assertResult("{\"obj\":{}}", "obj", "{}");
    }

    public void testEmptyArray() {
        assertResult("{\"arr\":[]}", "arr", "[]");
    }

    public void testEmptyString() {
        assertResult("{\"val\":\"\"}", "val", "");
    }

    public void testEmptyJsonObject() {
        assertWarningResult("{}", "anything", "path [anything] does not exist");
    }

    public void testEmptyInput() {
        assertWarningResult("", "$", "invalid JSON input");
    }

    // --- Type mismatch ---

    public void testKeyNavigationIntoArray() {
        // Path expects an object key but the value is an array
        assertWarningResult("{\"a\":[1,2]}", "a.b", "path [a.b] does not exist");
    }

    public void testIndexNavigationIntoObject() {
        // Path expects an array index but the value is an object
        assertWarningResult("{\"a\":{\"b\":1}}", "a[0]", "path [a[0]] does not exist");
    }

    public void testKeyNavigationIntoScalar() {
        // Path expects further nesting but the value is a scalar
        assertWarningResult("{\"a\":42}", "a.b", "path [a.b] does not exist");
    }

    // --- Array index out of bounds ---

    public void testArrayIndexExceedsBounds() {
        assertWarningResult("{\"a\":[1,2,3]}", "a[5]", "array index out of bounds");
    }

    // --- Unsupported JSONPath features ---

    public void testWildcardDotStar() {
        // $.* is parsed as key "*" — not found
        assertWarningResult("{\"a\":1,\"b\":2}", "*", "path [*] does not exist");
    }

    public void testWildcardBracketStar() {
        // $[*] — "*" is not a valid array index, caught at parse time
        assertWarningResult("[1,2,3]", "$[*]", "Invalid JSON path [$[*]]: expected integer array index, got [*] at position 1");
    }

    public void testRecursiveDescent() {
        // $..name — leading dot after stripping "$." → invalid path
        assertWarningResult("{\"a\":{\"name\":1}}", "$..name", "Invalid JSON path [$..name]: path cannot start with a dot at position 2");
    }

    public void testRecursiveDescentBare() {
        // ..name without $ — leading dot → invalid path
        assertWarningResult("{\"a\":{\"name\":1}}", "..name", "Invalid JSON path [..name]: path cannot start with a dot at position 0");
    }

    public void testArraySlice() {
        // [0:3] — "0:3" is not a valid integer index, caught at parse time
        assertWarningResult(
            "{\"arr\":[1,2,3,4]}",
            "arr[0:3]",
            "Invalid JSON path [arr[0:3]]: expected integer array index, got [0:3] at position 3"
        );
    }

    public void testArraySliceWithStep() {
        // [::2] — "::2" is not a valid integer index, caught at parse time
        assertWarningResult(
            "{\"arr\":[1,2,3,4]}",
            "arr[::2]",
            "Invalid JSON path [arr[::2]]: expected integer array index, got [::2] at position 3"
        );
    }

    public void testFilterExpression() {
        // ?(@.price<10) — not a valid array index, caught at parse time
        assertWarningResult(
            "{\"items\":[{\"price\":5}]}",
            "items[?(@.price<10)]",
            "Invalid JSON path [items[?(@.price<10)]]: expected integer array index, got [?(@.price<10)] at position 5"
        );
    }

    public void testUnionMultipleIndices() {
        // [0,1] — "0,1" is not a valid integer index, caught at parse time
        assertWarningResult(
            "{\"arr\":[1,2,3]}",
            "arr[0,1]",
            "Invalid JSON path [arr[0,1]]: expected integer array index, got [0,1] at position 3"
        );
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

    // --- Helper methods ---

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

    private String extract(String json, String path) {
        return extractFromBytes(new BytesRef(json), path);
    }

    private void assertResult(String json, String path, String expected) {
        String result = extract(json, path);
        assertThat(result, equalTo(expected));
    }

    private void assertNullResult(String json, String path) {
        String result = extract(json, path);
        assertThat(result, nullValue());
    }

    private void assertWarningResult(String json, String path, String warningMessage) {
        String result = extract(json, path);
        assertNull(result);
        assertWarnings(
            "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
            "Line -1:-1: java.lang.IllegalArgumentException: " + warningMessage
        );
    }

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
