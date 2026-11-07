/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.timeseries;

import org.apache.lucene.util.BytesRef;
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
import org.elasticsearch.xpack.esql.core.util.TimeSeriesMetadata;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToString;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.FoldNull;
import org.junit.After;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.compute.data.BlockUtils.fromArrayRow;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.unboundLogicalOptimizerContext;
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
            assertEquals(empty == null ? null : "{}", evaluate(new ToString(Source.EMPTY, METADATA, TEST_CFG), empty));
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
        var empty = metadata("{}");
        assertEquals("{}", evaluate(new ToString(Source.EMPTY, empty, TEST_CFG), (Object) null));
        assertEquals(new BytesRef("{}"), new ToString(Source.EMPTY, empty, TEST_CFG).fold(FoldContext.small()));
    }

    /** The keyword {@code _timeseries} source converts to typed metadata named as PromQL labels; null stays null. */
    public void testToTimeSeries() {
        var cast = new ToTimeSeries(Source.EMPTY, field("source", DataType.KEYWORD));
        assertEquals(
            "{\"attributes.cpu\":\"0\",\"cluster\":\"prod\",\"region\":[\"eu\",\"us\"]}",
            evaluate(
                new ToString(Source.EMPTY, cast, TEST_CFG),
                new BytesRef("{\"labels\":{\"cluster\":\"prod\",\"region\":[\"eu\",\"us\"]},\"attributes\":{\"cpu\":\"0\"}}")
            )
        );
        assertEquals(DataType.TIMESERIES_DATA_TYPE, cast.dataType());
        assertNull(evaluate(cast, (Object) null));
    }

    /** Removing a label is idempotent, including repeated and absent names, and an empty label reads as absent. */
    public void testRemovals() {
        BytesRef metadata = new BytesRef("{\"pod\":\"a\",\"job\":\"api\"}");
        assertEquals(
            "{\"job\":\"api\"}",
            evaluate(new TimeSeriesUnset(Source.EMPTY, METADATA, List.of(label("pod"), label("pod"), label("missing"))), metadata)
        );
        assertEquals(
            "{\"job\":\"api\",\"pod\":\"a\"}",
            evaluate(new TimeSeriesUnset(Source.EMPTY, METADATA, List.of(label("missing"))), metadata)
        );
        assertNull(evaluate(new TimeSeriesGet(Source.EMPTY, METADATA, label("pod")), new BytesRef("{\"pod\":\"\"}")));
    }

    /** Escaping and non-ASCII data survive reading and rendering without changing label identity. */
    public void testEscapedValues() {
        String name = "label\"\\\n";
        String value = "value\"\\\n\u03bb\ud83d\ude00";
        BytesRef encoded = encode(Map.of(name, value));
        assertEquals(encoded.utf8ToString(), evaluate(new ToString(Source.EMPTY, METADATA, TEST_CFG), encoded));
        assertEquals(value, evaluate(new TimeSeriesGet(Source.EMPTY, METADATA, label(name)), encoded));
    }

    /** Metadata shared through ordinals evaluates per row like any other block. */
    public void testOrdinalMetadata() {
        for (Expression expression : List.of(
            new TimeSeriesUnset(Source.EMPTY, METADATA, List.of(label("pod"))),
            new ToString(Source.EMPTY, METADATA, TEST_CFG)
        )) {
            DriverContext context = driverContext();
            try (
                BytesRefVector.Builder dictionary = context.blockFactory().newBytesRefVectorBuilder(2);
                IntBlock.Builder ordinals = context.blockFactory().newIntBlockBuilder(3)
            ) {
                dictionary.appendBytesRef(new BytesRef("{\"job\":\"api\",\"pod\":\"a\"}"));
                dictionary.appendBytesRef(new BytesRef("{\"job\":\"db\"}"));
                ordinals.appendInt(0).appendInt(1).appendInt(0);
                Page page = new Page(new OrdinalBytesRefBlock(ordinals.build(), dictionary.build()));
                try (ExpressionEvaluator evaluator = AbstractScalarFunctionTestCase.evaluator(expression).get(context)) {
                    try (BytesRefBlock result = (BytesRefBlock) evaluator.eval(page)) {
                        String first = expression instanceof ToString ? "{\"job\":\"api\",\"pod\":\"a\"}" : "{\"job\":\"api\"}";
                        assertEquals(first, result.getBytesRef(0, new BytesRef()).utf8ToString());
                        assertEquals("{\"job\":\"db\"}", result.getBytesRef(1, new BytesRef()).utf8ToString());
                        assertEquals(first, result.getBytesRef(2, new BytesRef()).utf8ToString());
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
                new TimeSeriesUnset(Source.EMPTY, METADATA, List.of(label("pod"))),
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
            new TimeSeriesUnset(Source.EMPTY, METADATA, List.of(label("pod")))
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
    }

    /** Breaker-accounted metadata is not subject to the arbitrary limit for unaccounted scalar strings. */
    public void testLargeResult() {
        String value = "x".repeat((1 << 20) + 128);
        BytesRef metadata = encode(Map.of("pod", value, "job", "api"));
        assertEquals("{\"pod\":\"" + value + "\"}", evaluate(new TimeSeriesUnset(Source.EMPTY, METADATA, List.of(label("job"))), metadata));
        assertEquals(
            "{\"pod\":\"" + value + "\"}",
            evaluate(new ToTimeSeries(Source.EMPTY, field("source", DataType.KEYWORD)), new BytesRef("{\"pod\":\"" + value + "\"}"))
        );
    }

    /** A breaker trip releases evaluated inputs, row scratch, and partially constructed output. */
    public void testCircuitBreakerFailureReleasesMemory() {
        BytesRef large = encode(Map.of("pod", "x".repeat(16 * 1024)));
        for (Expression expression : List.of(
            new TimeSeriesUnset(Source.EMPTY, METADATA, List.of(label("job"))),
            new ToString(Source.EMPTY, METADATA, TEST_CFG),
            new ToTimeSeries(Source.EMPTY, field("source", DataType.KEYWORD))
        )) {
            DriverContext context = driverContext(ByteSizeValue.ofKb(32));
            Page page = new Page(fromArrayRow(context.blockFactory(), large));
            try (ExpressionEvaluator evaluator = AbstractScalarFunctionTestCase.evaluator(expression).get(context)) {
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
        var metadata = metadata("{\"pod\":\"a\"}");
        assertEquals(new BytesRef("a"), new TimeSeriesGet(Source.EMPTY, metadata, label("pod")).fold(FoldContext.small()));
        assertEquals(new BytesRef("{}"), new TimeSeriesUnset(Source.EMPTY, metadata, List.of(label("pod"))).fold(FoldContext.small()));
        assertEquals(new BytesRef("{\"pod\":\"a\"}"), new ToString(Source.EMPTY, metadata, TEST_CFG).fold(FoldContext.small()));
    }

    /** Folding and the existing label conversion retain every value in a derived numeric label. */
    public void testFoldingMultipleValues() {
        var metadata = metadata("{\"shard\":[\"2\",\"10\"]}");
        assertEquals(
            List.of(new BytesRef("2"), new BytesRef("10")),
            new TimeSeriesGet(Source.EMPTY, metadata, label("shard")).fold(FoldContext.small())
        );
        assertEquals(
            List.of(2L, 10L),
            TimeSeriesGet.create(Source.EMPTY, metadata, "shard", DataType.LONG, TEST_CFG).fold(FoldContext.small())
        );
    }

    /** Generic conditional expressions retain opaque metadata until explicit rendering. */
    public void testTypedConditionalExpressions() {
        var fallback = metadata("{\"pod\":\"fallback\"}");
        var coalesce = new Coalesce(Source.EMPTY, METADATA, List.of(fallback));
        var rendered = new ToString(Source.EMPTY, coalesce, TEST_CFG);
        assertEquals("{\"pod\":\"fallback\"}", evaluate(rendered, (Object) null));
        assertEquals("{\"pod\":\"original\"}", evaluate(rendered, new BytesRef("{\"pod\":\"original\"}")));

        var unset = new TimeSeriesUnset(Source.EMPTY, METADATA, List.of(label("job")));
        rendered = new ToString(Source.EMPTY, new Coalesce(Source.EMPTY, unset, List.of(fallback)), TEST_CFG);
        assertEquals("{}", evaluate(rendered, (Object) null));

        var conditional = new Case(Source.EMPTY, field("condition", DataType.BOOLEAN), List.of(METADATA, fallback));
        rendered = new ToString(Source.EMPTY, conditional, TEST_CFG);
        assertEquals("{\"pod\":\"original\"}", evaluate(rendered, true, new BytesRef("{\"pod\":\"original\"}")));
        assertEquals("{\"pod\":\"fallback\"}", evaluate(rendered, false, new BytesRef("{\"pod\":\"original\"}")));
    }

    /** Typed metadata holding {@code json}, which is its canonical encoding. */
    private static Literal metadata(String json) {
        return new Literal(Source.EMPTY, new BytesRef(json), DataType.TIMESERIES_DATA_TYPE);
    }

    private static BytesRef encode(Map<String, String> labels) {
        var output = new ByteArrayOutputStream();
        try {
            TimeSeriesMetadata.of(labels).writeTo(output);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        return new BytesRef(output.toByteArray());
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
