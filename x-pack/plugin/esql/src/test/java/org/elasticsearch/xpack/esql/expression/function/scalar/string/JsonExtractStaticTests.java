/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
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
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.junit.After;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Static tests for {@link JsonExtract} that cover edge cases difficult to exercise
 * through the randomized {@link org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase} framework.
 * <p>
 * These tests directly invoke the evaluator to test path parsing edge cases,
 * large JSON inputs, Unicode handling, and constant-path specialization behavior.
 */
public class JsonExtractStaticTests extends ESTestCase {

    // --- Path parsing edge cases ---

    public void testEmptyPath() {
        assertWarningResult("{\"a\":1}", "", "invalid path: empty path");
    }

    public void testConsecutiveDots() {
        assertWarningResult("{\"a\":{\"b\":1}}", "a..b", "invalid path [a..b]");
    }

    public void testTrailingDot() {
        assertWarningResult("{\"a\":1}", "a.", "invalid path [a.]");
    }

    public void testLeadingDot() {
        assertWarningResult("{\"a\":1}", ".a", "invalid path [.a]");
    }

    public void testEmptyBrackets() {
        assertWarningResult("{\"a\":[1,2]}", "a[]", "invalid path [a[]]");
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
        String expectedToString = "JsonExtractConstantEvaluator[str=Attribute[channel=0], path=name]";
        var evaluatorFactory = AbstractScalarFunctionTestCase.evaluator(
            new JsonExtract(source, field("str", DataType.KEYWORD), new Literal(source, new BytesRef("name"), DataType.KEYWORD))
        );
        assertThat(evaluatorFactory.toString(), equalTo(expectedToString));
    }

    public void testNonConstantPathProducesGenericEvaluator() {
        Source source = Source.synthetic("json_extract");
        String expectedToString = "JsonExtractEvaluator[str=Attribute[channel=0], path=Attribute[channel=1]]";
        var evaluatorFactory = AbstractScalarFunctionTestCase.evaluator(
            new JsonExtract(source, field("str", DataType.KEYWORD), field("path", DataType.KEYWORD))
        );
        assertThat(evaluatorFactory.toString(), equalTo(expectedToString));
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

    // --- Helper methods ---

    private String extract(String json, String path) {
        try (
            var eval = AbstractScalarFunctionTestCase.evaluator(
                new JsonExtract(Source.EMPTY, field("str", DataType.KEYWORD), field("path", DataType.KEYWORD))
            ).get(driverContext());
            Block block = eval.eval(row(List.of(new BytesRef(json), new BytesRef(path))))
        ) {
            return block.isNull(0) ? null : ((BytesRef) BlockUtils.toJavaObject(block, 0)).utf8ToString();
        }
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

    /**
     * The following fields and methods were borrowed from AbstractScalarFunctionTestCase
     */
    private final List<CircuitBreaker> breakers = Collections.synchronizedList(new ArrayList<>());

    private static Page row(List<Object> values) {
        return new Page(1, BlockUtils.fromListRow(TestBlockFactory.getNonBreakingInstance(), values));
    }

    private static FieldAttribute field(String name, DataType type) {
        return new FieldAttribute(Source.synthetic(name), name, new EsField(name, type, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
    }

    private DriverContext driverContext() {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(256)).withCircuitBreaking();
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        breakers.add(breaker);
        return new DriverContext(bigArrays, new BlockFactory(breaker, bigArrays), null);
    }

    @After
    public void allMemoryReleased() {
        for (CircuitBreaker breaker : breakers) {
            assertThat(breaker.getUsed(), equalTo(0L));
        }
    }
}
