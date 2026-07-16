/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Standalone non-parameterized tests for {@link JsonExtract}: routing assertions, null-source
 * handling, large/deep input scenarios, XContent encoding coverage across SMILE/CBOR/YAML, and
 * randomized happy paths. Kept separate from the parameterized {@link JsonExtractTests} so the
 * {@code repeat-changed-tests} runner at {@code -Dtests.iters=100} stays under the default 512m
 * heap by capping per-class JUnit descriptor count.
 */
public class JsonExtractStaticTests extends ESTestCase {

    private final List<CircuitBreaker> breakers = Collections.synchronizedList(new ArrayList<>());

    // --- Evaluator routing ---

    public void testConstantPathProducesConstantEvaluator() {
        Source source = Source.synthetic("json_extract");
        var factory = AbstractScalarFunctionTestCase.evaluator(
            new JsonExtract(source, field("str", DataType.KEYWORD), new Literal(source, new BytesRef("name"), DataType.KEYWORD))
        );
        assertThat(factory, instanceOf(JsonExtractConstantEvaluator.Factory.class));
    }

    public void testNonConstantPathProducesGenericEvaluator() {
        Source source = Source.synthetic("json_extract");
        var factory = AbstractScalarFunctionTestCase.evaluator(
            new JsonExtract(source, field("str", DataType.KEYWORD), field("path", DataType.KEYWORD))
        );
        assertThat(factory, instanceOf(JsonExtractEvaluator.Factory.class));
    }

    public void testSourceTypedInputRoutesToSourceEvaluator() {
        Source source = Source.synthetic("json_extract");
        var factory = AbstractScalarFunctionTestCase.evaluator(
            new JsonExtract(source, field("str", DataType.SOURCE), field("path", DataType.KEYWORD))
        );
        assertThat(factory, instanceOf(JsonExtractSourceEvaluator.Factory.class));
    }

    public void testSourceTypedInputConstantPathRoutesToSourceConstantEvaluator() {
        Source source = Source.synthetic("json_extract");
        var factory = AbstractScalarFunctionTestCase.evaluator(
            new JsonExtract(source, field("str", DataType.SOURCE), new Literal(source, new BytesRef("name"), DataType.KEYWORD))
        );
        assertThat(factory, instanceOf(JsonExtractSourceConstantEvaluator.Factory.class));
    }

    // --- Null _source warning (function-level contract) ---

    public void testNullSourceConstantPathProducesWarning() {
        Source source = Source.synthetic("json_extract");
        var factory = AbstractScalarFunctionTestCase.evaluator(
            new JsonExtract(source, field("str", DataType.SOURCE), new Literal(source, new BytesRef("name"), DataType.KEYWORD))
        );
        try (var eval = factory.get(driverContext()); Block block = eval.eval(row(Collections.singletonList(null)))) {
            assertThat(block.isNull(0), equalTo(true));
        }
        assertWarnings(
            "Line -1:-1: evaluation of [json_extract] failed, treating result as null. Only first 20 failures recorded.",
            "Line -1:-1: java.lang.IllegalStateException: " + NULL_SOURCE_EXPECTED_MESSAGE
        );
    }

    public void testNullSourceNonConstantPathProducesWarning() {
        Source source = Source.synthetic("json_extract");
        var factory = AbstractScalarFunctionTestCase.evaluator(
            new JsonExtract(source, field("str", DataType.SOURCE), field("path", DataType.KEYWORD))
        );
        try (var eval = factory.get(driverContext()); Block block = eval.eval(row(Arrays.asList(null, new BytesRef("name"))))) {
            assertThat(block.isNull(0), equalTo(true));
        }
        assertWarnings(
            "Line -1:-1: evaluation of [json_extract] failed, treating result as null. Only first 20 failures recorded.",
            "Line -1:-1: java.lang.IllegalStateException: " + NULL_SOURCE_EXPECTED_MESSAGE
        );
    }

    private static final String NULL_SOURCE_EXPECTED_MESSAGE =
        "_source is null; this typically means the index has _source disabled in its mapping";

    public void testSourceTypedExtractionHappyPath() {
        Source source = Source.synthetic("json_extract");
        var factory = AbstractScalarFunctionTestCase.evaluator(
            new JsonExtract(source, field("str", DataType.SOURCE), field("path", DataType.KEYWORD))
        );
        try (
            var eval = factory.get(driverContext());
            Block block = eval.eval(row(List.of(new BytesRef("{\"name\":\"Alice\"}"), new BytesRef("name"))))
        ) {
            assertThat(block.isNull(0), equalTo(false));
            assertThat(((BytesRef) BlockUtils.toJavaObject(block, 0)).utf8ToString(), equalTo("Alice"));
        }
    }

    // --- Large / deep JSON ---

    public void testDeeplyNestedPath() {
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
        StringBuilder json = new StringBuilder("{\"items\":[");
        for (int i = 0; i < 1000; i++) {
            if (i > 0) json.append(",");
            json.append(i);
        }
        json.append("]}");
        assertResult(json.toString(), "items[999]", "999");
    }

    public void testLargeJsonObject() {
        StringBuilder json = new StringBuilder("{");
        for (int i = 0; i < 1000; i++) {
            if (i > 0) json.append(",");
            json.append("\"key").append(i).append("\":").append(i);
        }
        json.append("}");
        assertResult(json.toString(), "key999", "999");
    }

    // --- XContent encoding coverage ---
    // _source preserves the original encoding from indexing. These tests verify that
    // doExtract handles all Elasticsearch content types (JSON, SMILE, CBOR, YAML).

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

    // --- Randomized happy paths across all XContent encodings ---

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

    public void testRandomArrayIndexAllEncodings() throws IOException {
        int arraySize = randomIntBetween(1, 20);
        List<String> items = new ArrayList<>();
        for (int i = 0; i < arraySize; i++) {
            items.add(randomAlphaOfLengthBetween(1, 20));
        }
        int targetIndex = randomIntBetween(0, arraySize - 1);
        forAllEncodings(Map.of("items", items), "items[" + targetIndex + "]", items.get(targetIndex));
    }

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

    // --- Helpers ---

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
            var eval = AbstractScalarFunctionTestCase.evaluator(
                new JsonExtract(Source.EMPTY, field("str", DataType.KEYWORD), field("path", DataType.KEYWORD))
            ).get(driverContext());
            Block block = eval.eval(row(List.of(bytes, new BytesRef(path))))
        ) {
            return block.isNull(0) ? null : ((BytesRef) BlockUtils.toJavaObject(block, 0)).utf8ToString();
        }
    }

    private void assertResult(String json, String path, String expected) {
        assertThat(extractFromBytes(new BytesRef(json), path), equalTo(expected));
    }

    private static Page row(List<Object> values) {
        return new Page(1, BlockUtils.fromListRow(TestBlockFactory.getNonBreakingInstance(), values));
    }

    private static FieldAttribute field(String name, DataType type) {
        return new FieldAttribute(Source.synthetic(name), name, new EsField(name, type, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
    }

    private DriverContext driverContext() {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(256)).withCircuitBreaking();
        breakers.add(bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST));
        return new DriverContext(bigArrays, BlockFactory.builder(bigArrays).build(), null);
    }

    @After
    public void allMemoryReleased() {
        for (CircuitBreaker breaker : breakers) {
            assertThat(breaker.getUsed(), equalTo(0L));
        }
    }
}
