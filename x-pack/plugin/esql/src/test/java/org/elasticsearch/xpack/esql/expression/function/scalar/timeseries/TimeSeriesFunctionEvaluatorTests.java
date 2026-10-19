/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.timeseries;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.routing.TsidBuilder;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToString;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.FoldNull;
import org.junit.After;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.compute.data.BlockUtils.fromArrayRow;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.unboundLogicalOptimizerContext;
import static org.elasticsearch.xpack.esql.core.util.NumericUtils.asLongUnsigned;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.stringToIP;
import static org.hamcrest.Matchers.containsString;

/** Executes the public functions through real evaluators, including row-dependent dictionary overlays. */
public class TimeSeriesFunctionEvaluatorTests extends ESTestCase {
    private static final FieldAttribute METADATA = field("metadata", DataType.TIMESERIES_DATA_TYPE);
    private final List<CircuitBreaker> breakers = new ArrayList<>();

    /** Metadata operations treat null as empty; rendering preserves nulls and missing labels remain nullable. */
    public void testEmptyMetadata() {
        for (Object empty : new Object[] { null, new BytesRef("{}"), new BytesRef("{\"empty\":\"\",\"null\":null}") }) {
            assertNull(evaluate(new TimeSeriesGet(Source.EMPTY, METADATA, label("pod")), empty));
            assertEquals("{}", evaluate(new TimeSeriesUnset(Source.EMPTY, METADATA, List.of(label("pod"))), empty));
            assertEquals(hash(Map.of()), evaluateBytes(new TimeSeriesHash(Source.EMPTY, METADATA, List.of()), empty));
            assertEquals(empty == null ? null : "{}", evaluate(new ToString(Source.EMPTY, METADATA, TEST_CFG), empty));
            assertEquals("{\"pod\":\"a\"}", evaluate(new TimeSeriesSet(Source.EMPTY, METADATA, label("pod"), label("a")), empty));
        }
    }

    /** Rendering preserves nulls while an encoded empty label set remains a printable value. */
    public void testToStringNullAndEmptyMetadata() {
        Expression untypedNull = new FoldNull().rule(new ToString(Source.EMPTY, Literal.NULL, TEST_CFG), unboundLogicalOptimizerContext());
        assertEquals(DataType.KEYWORD, untypedNull.dataType());
        assertNull(evaluate(untypedNull, (Object) null));
        assertNull(untypedNull.fold(FoldContext.small()));

        var typedNull = new Literal(Source.EMPTY, null, DataType.TIMESERIES_DATA_TYPE);
        var rendered = new ToString(Source.EMPTY, typedNull, TEST_CFG);
        assertNull(evaluate(rendered, (Object) null));
        assertTrue(rendered.foldable());
        assertNull(rendered.fold(FoldContext.small()));
        var empty = new Literal(Source.EMPTY, new BytesRef("{}"), DataType.TIMESERIES_DATA_TYPE);
        assertEquals("{}", evaluate(new ToString(Source.EMPTY, empty, TEST_CFG), (Object) null));
        assertEquals(new BytesRef("{}"), new ToString(Source.EMPTY, empty, TEST_CFG).fold(FoldContext.small()));
    }

    /** Removing a label is idempotent, including repeated names and absent or empty replacements. */
    public void testRemovalsAndNoOpTransforms() {
        BytesRef metadata = new BytesRef("{\"pod\":\"a\",\"job\":\"api\"}");
        for (Expression value : List.of(Literal.NULL, label(""))) {
            assertEquals("{\"job\":\"api\"}", evaluate(new TimeSeriesSet(Source.EMPTY, METADATA, label("pod"), value), metadata));
        }
        assertEquals(
            "{\"job\":\"api\"}",
            evaluate(new TimeSeriesUnset(Source.EMPTY, METADATA, List.of(label("pod"), label("pod"), label("missing"))), metadata)
        );
        assertEquals(
            "{\"job\":\"api\",\"pod\":\"a\"}",
            evaluate(new TimeSeriesSet(Source.EMPTY, METADATA, label("pod"), label("a")), metadata)
        );
        assertEquals(
            "{\"job\":\"api\",\"pod\":\"a\"}",
            evaluate(new TimeSeriesUnset(Source.EMPTY, METADATA, List.of(label("missing"))), metadata)
        );
        assertNull(evaluate(new TimeSeriesGet(Source.EMPTY, METADATA, label("pod")), new BytesRef("{\"pod\":\"\"}")));
    }

    /** Promoted columns and encoded labels must produce the same ingest hash of the complete label set. */
    public void testCanonicalKeysAndOverlays() {
        var hash = new TimeSeriesHash(Source.EMPTY, METADATA, List.of());
        BytesRef key = evaluateBytes(hash, new BytesRef("{ \"pod\": \"a\", \"job\": \"api\" }"));
        assertEquals(16, key.length);
        assertEquals(hash(Map.of("job", "api", "pod", "a")), key);
        assertEquals(key, evaluateBytes(hash, new BytesRef("{\"job\":\"api\",\"pod\":\"a\"}")));
        var overlay = new TimeSeriesHash(Source.EMPTY, METADATA, List.of(label("pod"), label("a")));
        assertEquals(key, evaluateBytes(overlay, new BytesRef("{\"job\":\"api\",\"pod\":\"old\"}")));
        assertEquals(key, evaluateBytes(overlay, new BytesRef("{\"job\":\"api\"}")));
        var promoted = new TimeSeriesHash(Source.EMPTY, METADATA, List.of(label("pod"), label("a"), label("job"), label("api")));
        assertEquals(key, evaluateBytes(promoted, (Object) null));
        for (Expression removed : List.of(Literal.NULL, label(""))) {
            assertEquals(
                hash(Map.of("job", "api")),
                evaluateBytes(
                    new TimeSeriesHash(Source.EMPTY, METADATA, List.of(label("pod"), removed)),
                    new BytesRef("{\"pod\":\"a\",\"job\":\"api\"}")
                )
            );
        }
        assertEquals(
            "{\"job\":\"api\",\"pod\":\"a\"}",
            evaluate(
                new ToString(Source.EMPTY, new TimeSeriesSet(Source.EMPTY, METADATA, label("pod"), label("a")), TEST_CFG),
                new BytesRef("{\"job\":\"api\",\"pod\":\"old\"}")
            )
        );
        assertNotEquals(key, evaluateBytes(hash, new BytesRef("{\"job\":\"api\",\"pod\":\"b\"}")));
    }

    /** Values retain their label representation, including unsigned values and encoded IP addresses. */
    public void testTypedValues() {
        assertValue(DataType.KEYWORD, new BytesRef("a"), "a");
        assertValue(DataType.TEXT, new BytesRef("a"), "a");
        assertValue(DataType.BOOLEAN, false, "false");
        assertValue(DataType.INTEGER, 0, "0");
        assertValue(DataType.LONG, Long.MIN_VALUE, Long.toString(Long.MIN_VALUE));
        assertValue(DataType.UNSIGNED_LONG, asLongUnsigned(BigInteger.ZERO), "0");
        assertValue(DataType.UNSIGNED_LONG, asLongUnsigned(BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE)), "18446744073709551615");
        assertValue(DataType.DOUBLE, -0.0, "-0.0");
        assertValue(DataType.DOUBLE, 1.25, "1.25");
        assertValue(DataType.IP, stringToIP("192.0.2.1"), "192.0.2.1");
        assertValue(DataType.IP, stringToIP("2001:db8::1"), "2001:db8::1");
    }

    /** Escaping and non-ASCII data must survive SET/GET without changing label identity. */
    public void testEscapedValues() {
        String name = "label\"\\\n";
        String value = "value\"\\\nλ😀";
        var set = new TimeSeriesSet(Source.EMPTY, METADATA, label(name), label(value));
        String encoded = evaluate(set, (Object) null);
        assertEquals(encoded, evaluate(new ToString(Source.EMPTY, METADATA, TEST_CFG), new BytesRef(encoded)));
        assertEquals(encoded, evaluate(new ToString(Source.EMPTY, set, TEST_CFG), (Object) null));
        assertEquals(value, evaluate(new TimeSeriesGet(Source.EMPTY, METADATA, label(name)), new BytesRef(encoded)));
        assertEquals(
            hash(Map.of(name, value)),
            evaluateBytes(new TimeSeriesHash(Source.EMPTY, METADATA, List.of()), new BytesRef(encoded))
        );
    }

    /** Shared metadata ordinals do not imply shared values for promoted columns. */
    public void testRowDependentOverlaysOnOrdinalMetadata() {
        var value = field("value", DataType.KEYWORD);
        for (Expression expression : List.of(
            new TimeSeriesSet(Source.EMPTY, METADATA, label("pod"), value),
            new TimeSeriesHash(Source.EMPTY, METADATA, List.of(label("pod"), value)),
            new ToString(Source.EMPTY, new TimeSeriesSet(Source.EMPTY, METADATA, label("pod"), value), TEST_CFG)
        )) {
            DriverContext context = driverContext();
            try (
                BytesRefVector.Builder dictionary = context.blockFactory().newBytesRefVectorBuilder(1);
                IntBlock.Builder ordinals = context.blockFactory().newIntBlockBuilder(3);
                BytesRefBlock.Builder values = context.blockFactory().newBytesRefBlockBuilder(3)
            ) {
                dictionary.appendBytesRef(new BytesRef("{\"job\":\"api\"}"));
                ordinals.appendInt(0).appendInt(0).appendInt(0);
                values.appendBytesRef(new BytesRef("a")).appendBytesRef(new BytesRef("b")).appendNull();
                Page page = new Page(new OrdinalBytesRefBlock(ordinals.build(), dictionary.build()), values.build());
                try (ExpressionEvaluator evaluator = AbstractScalarFunctionTestCase.evaluator(expression).get(context)) {
                    try (BytesRefBlock result = (BytesRefBlock) evaluator.eval(page)) {
                        if (expression instanceof TimeSeriesHash) {
                            assertEquals(hash(Map.of("job", "api", "pod", "a")), result.getBytesRef(0, new BytesRef()));
                            assertEquals(hash(Map.of("job", "api", "pod", "b")), result.getBytesRef(1, new BytesRef()));
                            assertEquals(hash(Map.of("job", "api")), result.getBytesRef(2, new BytesRef()));
                        } else {
                            assertEquals("{\"job\":\"api\",\"pod\":\"a\"}", result.getBytesRef(0, new BytesRef()).utf8ToString());
                            assertEquals("{\"job\":\"api\",\"pod\":\"b\"}", result.getBytesRef(1, new BytesRef()).utf8ToString());
                            assertEquals("{\"job\":\"api\"}", result.getBytesRef(2, new BytesRef()).utf8ToString());
                        }
                    }
                } finally {
                    page.releaseBlocks();
                }
            }
        }
    }

    /** Invalid metadata must fail rather than silently merge series through a null result. */
    public void testMalformedMetadataFails() {
        for (String invalid : List.of("not JSON", "[]", "{\"pod\":3}", "{\"pod\":\"a\",\"pod\":\"b\"}", "{} {}")) {
            for (Expression expression : List.of(
                new TimeSeriesHash(Source.EMPTY, METADATA, List.of()),
                new ToString(Source.EMPTY, METADATA, TEST_CFG)
            )) {
                expectThrows(RuntimeException.class, () -> evaluateBytes(expression, new BytesRef(invalid)));
            }
        }
    }

    /** Metadata itself is one encoded value, even when the labels inside it contain multiple values. */
    public void testMultivaluedMetadataFails() {
        for (Expression expression : List.of(
            new TimeSeriesGet(Source.EMPTY, METADATA, label("pod")),
            new ToString(Source.EMPTY, METADATA, TEST_CFG)
        )) {
            var error = expectThrows(
                IllegalArgumentException.class,
                () -> evaluate(expression, List.of(new BytesRef("{\"pod\":\"a\"}"), new BytesRef("{\"pod\":\"b\"}")))
            );
            assertThat(error.getMessage(), containsString("must be single-valued"));
        }
    }

    /** GET exposes all members as one multivalued position, while UNSET removes the complete label. */
    public void testMultipleValuesReadRenderAndRemove() {
        var metadata = new BytesRef("{\"pod\":[\"b\",\"a\",\"b\",\"\"],\"empty\":[null,\"\"],\"single\":[\"one\"]}");
        assertEquals(
            List.of(new BytesRef("b"), new BytesRef("a"), new BytesRef("b")),
            evaluateValues(new TimeSeriesGet(Source.EMPTY, METADATA, label("pod")), metadata)
        );
        assertEquals("one", evaluate(new TimeSeriesGet(Source.EMPTY, METADATA, label("single")), metadata));
        assertNull(evaluate(new TimeSeriesGet(Source.EMPTY, METADATA, label("empty")), metadata));
        assertEquals(
            "{\"pod\":[\"b\",\"a\",\"b\"],\"single\":\"one\"}",
            evaluate(new ToString(Source.EMPTY, METADATA, TEST_CFG), metadata)
        );
        assertEquals("{\"single\":\"one\"}", evaluate(new TimeSeriesUnset(Source.EMPTY, METADATA, List.of(label("pod"))), metadata));
        assertEquals(
            hashValues(Map.of("pod", List.of("b", "a", "b"), "single", List.of("one"))),
            evaluateBytes(new TimeSeriesHash(Source.EMPTY, METADATA, List.of()), metadata)
        );
    }

    /** SET and HASH consume every typed value without joining values or changing their order. */
    public void testMultipleTypedValues() {
        assertValues(DataType.KEYWORD, List.of(new BytesRef("b"), new BytesRef("a"), new BytesRef("b")), List.of("b", "a", "b"));
        assertValues(DataType.BOOLEAN, List.of(true, false), List.of("true", "false"));
        assertValues(DataType.INTEGER, List.of(2, 10), List.of("2", "10"));
        assertValues(
            DataType.LONG,
            List.of(Long.MIN_VALUE, Long.MAX_VALUE),
            List.of(Long.toString(Long.MIN_VALUE), Long.toString(Long.MAX_VALUE))
        );
        assertValues(
            DataType.UNSIGNED_LONG,
            List.of(asLongUnsigned(BigInteger.ZERO), asLongUnsigned(BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE))),
            List.of("0", "18446744073709551615")
        );
        assertValues(DataType.DOUBLE, List.of(-0.0, 1.25), List.of("-0.0", "1.25"));
        assertValues(DataType.IP, List.of(stringToIP("192.0.2.1"), stringToIP("2001:db8::1")), List.of("192.0.2.1", "2001:db8::1"));
    }

    /** An overlay replaces the whole prior value list; null and empty members do not become labels. */
    public void testMultipleValueOverlaysNormalizeEmptyMembers() {
        var value = field("value", DataType.KEYWORD);
        var set = new TimeSeriesSet(Source.EMPTY, METADATA, label("pod"), value);
        var metadata = new BytesRef("{\"pod\":[\"old\",\"values\"],\"job\":\"api\"}");
        assertEquals("{\"job\":\"api\",\"pod\":\"new\"}", evaluate(set, metadata, List.of(new BytesRef(""), new BytesRef("new"))));
        assertEquals("{\"job\":\"api\"}", evaluate(set, metadata, List.of(new BytesRef(""), new BytesRef(""))));
        assertEquals("{\"job\":\"api\"}", evaluate(set, metadata, null));
        assertEquals(
            hash(Map.of("job", "api", "pod", "new")),
            evaluateBytes(
                new TimeSeriesHash(Source.EMPTY, METADATA, List.of(label("pod"), value)),
                metadata,
                List.of(new BytesRef(""), new BytesRef("new"))
            )
        );
    }

    /** Breaker-accounted metadata is not subject to the arbitrary limit for unaccounted scalar strings. */
    public void testLargeResult() {
        var set = new TimeSeriesSet(Source.EMPTY, METADATA, label("pod"), field("value", DataType.KEYWORD));
        String value = "x".repeat((1 << 20) + 128);
        assertEquals("{\"pod\":\"" + value + "\"}", evaluate(set, null, new BytesRef(value)));
    }

    /** A breaker trip releases evaluated inputs, row scratch, and partially constructed output. */
    public void testCircuitBreakerFailureReleasesMemory() {
        var set = new TimeSeriesSet(Source.EMPTY, METADATA, label("pod"), field("value", DataType.KEYWORD));
        for (Object value : List.of(
            new BytesRef("x".repeat(1024)),
            List.of(new BytesRef("a"), new BytesRef("x".repeat(1024))),
            Collections.nCopies(256, new BytesRef(""))
        )) {
            DriverContext context = driverContext(ByteSizeValue.ofKb(32));
            Page page = new Page(fromArrayRow(context.blockFactory(), null, value));
            try (ExpressionEvaluator evaluator = AbstractScalarFunctionTestCase.evaluator(set).get(context)) {
                expectThrows(CircuitBreakingException.class, () -> {
                    try (var unexpected = evaluator.eval(page)) {
                        // Close an unexpected successful result so a failed assertion does not leak it.
                    }
                });
            } finally {
                page.releaseBlocks();
            }
        }
    }

    /** Constant expressions use the same execution semantics as column-backed expressions. */
    public void testFolding() {
        var set = new TimeSeriesSet(Source.EMPTY, Literal.NULL, label("pod"), label("a"));
        assertTrue(set.foldable());
        assertEquals(new BytesRef("{\"pod\":\"a\"}"), set.fold(FoldContext.small()));
        assertEquals(new BytesRef("a"), new TimeSeriesGet(Source.EMPTY, set, label("pod")).fold(FoldContext.small()));
        assertEquals(new BytesRef("{}"), new TimeSeriesUnset(Source.EMPTY, set, List.of(label("pod"))).fold(FoldContext.small()));
        var hash = new TimeSeriesHash(Source.EMPTY, set, List.of());
        assertEquals(DataType.TSID_DATA_TYPE, hash.dataType());
        assertEquals(hash(Map.of("pod", "a")), hash.fold(FoldContext.small()));
        assertEquals(new BytesRef("{\"pod\":\"a\"}"), new ToString(Source.EMPTY, set, TEST_CFG).fold(FoldContext.small()));
    }

    /** Folding and the existing label conversion retain every value in a derived numeric label. */
    public void testFoldingMultipleValues() {
        var set = new TimeSeriesSet(Source.EMPTY, Literal.NULL, label("shard"), new Literal(Source.EMPTY, List.of(2L, 10L), DataType.LONG));
        assertEquals(new BytesRef("{\"shard\":[\"2\",\"10\"]}"), set.fold(FoldContext.small()));
        assertEquals(
            List.of(new BytesRef("2"), new BytesRef("10")),
            new TimeSeriesGet(Source.EMPTY, set, label("shard")).fold(FoldContext.small())
        );
        assertEquals(List.of(2L, 10L), LabelEval.create(Source.EMPTY, set, "shard", DataType.LONG, TEST_CFG).fold(FoldContext.small()));
        assertEquals(
            hashValues(Map.of("shard", List.of("2", "10"))),
            new TimeSeriesHash(Source.EMPTY, set, List.of()).fold(FoldContext.small())
        );
    }

    /** Generic conditional expressions retain opaque metadata until explicit rendering. */
    public void testTypedConditionalExpressions() {
        var fallback = new TimeSeriesSet(Source.EMPTY, Literal.NULL, label("pod"), label("fallback"));
        var coalesce = new Coalesce(Source.EMPTY, METADATA, List.of(fallback));
        var rendered = new ToString(Source.EMPTY, coalesce, TEST_CFG);
        assertEquals("{\"pod\":\"fallback\"}", evaluate(rendered, (Object) null));
        assertEquals("{\"pod\":\"original\"}", evaluate(rendered, new BytesRef("{\"pod\":\"original\"}")));

        var set = new TimeSeriesSet(Source.EMPTY, METADATA, label("pod"), label("changed"));
        rendered = new ToString(Source.EMPTY, new Coalesce(Source.EMPTY, set, List.of(fallback)), TEST_CFG);
        assertEquals("{\"pod\":\"changed\"}", evaluate(rendered, (Object) null));

        var conditional = new Case(Source.EMPTY, field("condition", DataType.BOOLEAN), List.of(METADATA, fallback));
        rendered = new ToString(Source.EMPTY, conditional, TEST_CFG);
        assertEquals("{\"pod\":\"original\"}", evaluate(rendered, true, new BytesRef("{\"pod\":\"original\"}")));
        assertEquals("{\"pod\":\"fallback\"}", evaluate(rendered, false, new BytesRef("{\"pod\":\"original\"}")));
    }

    private void assertValue(DataType type, Object value, String expected) {
        var set = new TimeSeriesSet(Source.EMPTY, METADATA, label("value"), field("value", type));
        String encoded = evaluate(set, null, value);
        assertEquals(expected, evaluate(new TimeSeriesGet(Source.EMPTY, METADATA, label("value")), new BytesRef(encoded)));
        assertEquals(
            hash(Map.of("value", expected)),
            evaluateBytes(new TimeSeriesHash(Source.EMPTY, METADATA, List.of(label("value"), field("value", type))), null, value)
        );
        assertEquals(
            hash(Map.of("value", expected)),
            evaluateBytes(new TimeSeriesHash(Source.EMPTY, METADATA, List.of()), new BytesRef(encoded))
        );
    }

    private void assertValues(DataType type, List<?> values, List<String> expected) {
        var value = field("value", type);
        var set = new TimeSeriesSet(Source.EMPTY, METADATA, label("value"), value);
        String encoded = evaluate(set, null, values);
        assertEquals(
            expected.stream().map(BytesRef::new).toList(),
            evaluateValues(new TimeSeriesGet(Source.EMPTY, METADATA, label("value")), new BytesRef(encoded))
        );
        assertEquals(encoded, evaluate(new ToString(Source.EMPTY, set, TEST_CFG), null, values));
        BytesRef hash = hashValues(Map.of("value", expected));
        assertEquals(hash, evaluateBytes(new TimeSeriesHash(Source.EMPTY, METADATA, List.of(label("value"), value)), null, values));
        assertEquals(hash, evaluateBytes(new TimeSeriesHash(Source.EMPTY, METADATA, List.of()), new BytesRef(encoded)));
    }

    private String evaluate(Expression expression, Object... values) {
        BytesRef result = evaluateBytes(expression, values);
        return result == null ? null : result.utf8ToString();
    }

    private BytesRef evaluateBytes(Expression expression, Object... values) {
        List<BytesRef> result = evaluateValues(expression, values);
        if (result == null) {
            return null;
        }
        assertEquals(1, result.size());
        return result.getFirst();
    }

    private List<BytesRef> evaluateValues(Expression expression, Object... values) {
        DriverContext context = driverContext();
        Page page = new Page(fromArrayRow(context.blockFactory(), values));
        try (ExpressionEvaluator evaluator = AbstractScalarFunctionTestCase.evaluator(expression).get(context)) {
            try (BytesRefBlock result = (BytesRefBlock) evaluator.eval(page)) {
                assertEquals(1, result.getPositionCount());
                if (result.isNull(0)) {
                    return null;
                }
                var output = new ArrayList<BytesRef>();
                for (int i = 0; i < result.getValueCount(0); i++) {
                    output.add(BytesRef.deepCopyOf(result.getBytesRef(result.getFirstValueIndex(0) + i, new BytesRef())));
                }
                return output;
            }
        } finally {
            page.releaseBlocks();
        }
    }

    private static BytesRef hash(Map<String, String> labels) {
        var builder = new TsidBuilder(labels.size());
        labels.forEach(builder::addStringDimension);
        return new BytesRef(builder.hash().getBytes());
    }

    private static BytesRef hashValues(Map<String, List<String>> labels) {
        var builder = new TsidBuilder();
        labels.forEach((name, values) -> values.forEach(value -> builder.addStringDimension(name, value)));
        return new BytesRef(builder.hash().getBytes());
    }

    private static Literal label(String value) {
        return Literal.keyword(Source.EMPTY, value);
    }

    private static FieldAttribute field(String name, DataType type) {
        return new FieldAttribute(Source.synthetic(name), name, new EsField(name, type, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
    }

    private DriverContext driverContext() {
        return driverContext(ByteSizeValue.ofMb(256));
    }

    private DriverContext driverContext(ByteSizeValue limit) {
        BigArrays arrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, limit).withCircuitBreaking();
        breakers.add(arrays.breakerService().getBreaker(CircuitBreaker.REQUEST));
        return new DriverContext(arrays, BlockFactory.builder(arrays).build(), null);
    }

    /** Failures must release inputs, row scratch, and any partial output. */
    @After
    public void allMemoryReleased() {
        for (CircuitBreaker breaker : breakers) {
            assertEquals(0L, breaker.getUsed());
        }
    }
}
