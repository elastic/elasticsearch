/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.timeseries;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.junit.After;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

/**
 * The dictionary and constant paths of {@link AbstractTimeSeriesMetadataFunction.AbstractEvaluator}. A label container is
 * loaded once per series and page, so a page holds it dictionary encoded: a function of the label container alone runs once per
 * dictionary entry, or once for a constant one. Every case here is asserted to give, position for position, what the same
 * values give through the per-row path.
 */
public class TimeSeriesMetadataFunctionOrdinalTests extends ESTestCase {

    private static final String[] CONTAINERS = {
        "{\"cluster\":\"prod\",\"pod\":\"one\"}",
        "{\"cluster\":\"prod\",\"pod\":\"two\"}",
        "{\"cluster\":\"qa\",\"pod\":\"three\"}" };
    /** Label containers as remote write stores them, every label in a {@code labels} object. */
    private static final String[] NESTED_CONTAINERS = {
        "{\"labels\":{\"cluster\":\"prod\",\"pod\":\"one\"}}",
        "{\"labels\":{\"cluster\":\"prod\",\"pod\":\"two\"}}",
        "{\"labels\":{\"cluster\":\"qa\",\"pod\":\"three\"}}" };
    /** 15 rows over 3 entries: dense. */
    private static final Integer[] DENSE = { 0, 1, 0, 2, 1, 0, 1, 2, 0, 0, 1, 2, 2, 0, 1 };

    private final List<CircuitBreaker> breakers = Collections.synchronizedList(new ArrayList<>());

    public void testReadReusesOrdinals() {
        assertDictionaryPath(c -> new TimeSeriesMetadataPath(Source.EMPTY, c, keyword("pod")), CONTAINERS, DENSE, true);
    }

    public void testReadOfAbsentOrSeveralValuesKeepsRows() {
        // An absent label reads as null and a label of several values as one multi-valued position: neither fits a dictionary.
        String[] containers = { "{\"pod\":\"one\"}", "{\"cluster\":\"prod\"}", "{\"pod\":[\"one\",\"two\"]}" };
        assertDictionaryPath(c -> new TimeSeriesMetadataPath(Source.EMPTY, c, keyword("pod")), containers, DENSE, false);
    }

    public void testKeepReusesOrdinals() {
        assertDictionaryPath(
            c -> new TimeSeriesMetadataFilter(Source.EMPTY, c, List.of(keyword("cluster")), false),
            CONTAINERS,
            DENSE,
            true
        );
    }

    public void testDropReusesOrdinals() {
        assertDictionaryPath(
            c -> new TimeSeriesMetadataFilter(Source.EMPTY, c, List.of(keyword("pod"), keyword("absent")), true),
            CONTAINERS,
            DENSE,
            true
        );
    }

    public void testSetOfConstantReusesOrdinals() {
        assertDictionaryPath(
            c -> new TimeSeriesMetadataSet(Source.EMPTY, c, keyword("job"), keyword("api"), List.of("job")),
            CONTAINERS,
            DENSE,
            true
        );
    }

    public void testSetOfNullReusesOrdinals() {
        assertDictionaryPath(
            c -> new TimeSeriesMetadataSet(
                Source.EMPTY,
                c,
                keyword("pod"),
                new Literal(Source.EMPTY, null, DataType.KEYWORD),
                List.of("pod")
            ),
            CONTAINERS,
            DENSE,
            true
        );
    }

    public void testSetOfColumnKeepsRows() {
        // The written value changes per row, so the result is not a function of the label container alone.
        assertDictionaryPath(
            c -> new TimeSeriesMetadataSet(Source.EMPTY, c, keyword("job"), field("job", DataType.KEYWORD), List.of("job")),
            CONTAINERS,
            DENSE,
            false
        );
    }

    public void testNullContainerKeepsRows() {
        // A rewrite of a null label container is a label container, which reused ordinals would leave null.
        Integer[] ordinals = { 0, 1, null, 2, 0, null, 1, 2, 0, 1, 2, 0, 1, 2, 0 };
        assertDictionaryPath(
            c -> new TimeSeriesMetadataFilter(Source.EMPTY, c, List.of(keyword("pod")), true),
            CONTAINERS,
            ordinals,
            false
        );
        assertDictionaryPath(c -> new TimeSeriesMetadataPath(Source.EMPTY, c, keyword("pod")), CONTAINERS, ordinals, false);
    }

    public void testConstantEvaluatesOnce() {
        assertConstant(c -> new TimeSeriesMetadataPath(Source.EMPTY, c, keyword("pod")), CONTAINERS[0], true);
        assertConstant(c -> new TimeSeriesMetadataFilter(Source.EMPTY, c, List.of(keyword("pod")), true), CONTAINERS[0], true);
        assertConstant(
            c -> new TimeSeriesMetadataSet(Source.EMPTY, c, keyword("job"), keyword("api"), List.of("job")),
            CONTAINERS[0],
            true
        );
        // A read of an absent label is null on every row.
        assertConstant(c -> new TimeSeriesMetadataPath(Source.EMPTY, c, keyword("absent")), CONTAINERS[0], true);
        // A read of a label of several values is not one value, so it is evaluated per row.
        assertConstant(c -> new TimeSeriesMetadataPath(Source.EMPTY, c, keyword("pod")), "{\"pod\":[\"one\",\"two\"]}", false);
    }

    /**
     * Evaluates {@code function} over a page whose label container is the constant {@code labels}, and once over the same rows as
     * plain values, and asserts both give the same values, the first as a constant block when {@code constant}.
     */
    private void assertConstant(Function<Expression, Expression> function, String labels, boolean constant) {
        Expression expression = function.apply(field("labels", DataType.KEYWORD));
        int positions = DENSE.length;
        List<List<String>> once;
        try (
            Block result = evaluate(
                expression,
                driverContext().blockFactory().newConstantBytesRefBlockWith(new BytesRef(labels), positions),
                positions
            )
        ) {
            assertThat(result.asVector() != null && result.asVector().isConstant() || result.areAllValuesNull(), equalTo(constant));
            once = values(result);
        }
        String[] rows = new String[positions];
        Arrays.fill(rows, labels);
        try (Block result = evaluate(expression, plainBlock(rows), positions)) {
            assertThat(once, equalTo(values(result)));
        }
    }

    public void testNestedReadReusesOrdinals() {
        // A keyword _timeseries is the source's JSON object, with its objects: a label is named by its field path.
        assertDictionaryPath(c -> new TimeSeriesMetadataPath(Source.EMPTY, c, keyword("pod")), NESTED_CONTAINERS, DENSE, true);
    }

    public void testNestedDropReusesOrdinals() {
        assertDictionaryPath(
            c -> new TimeSeriesMetadataFilter(Source.EMPTY, c, List.of(keyword("pod")), true),
            NESTED_CONTAINERS,
            DENSE,
            true
        );
    }

    public void testNestedSetReusesOrdinals() {
        assertDictionaryPath(
            c -> new TimeSeriesMetadataSet(Source.EMPTY, c, keyword("job"), keyword("api"), List.of("labels", "job")),
            NESTED_CONTAINERS,
            DENSE,
            true
        );
    }

    public void testSparseKeepsRows() {
        Integer[] ordinals = { 0, 1, 2 };
        assertDictionaryPath(c -> new TimeSeriesMetadataPath(Source.EMPTY, c, keyword("pod")), CONTAINERS, ordinals, false);
    }

    /**
     * Evaluates {@code function} over the label containers {@code ordinals} pick out of {@code dictionary}, once dictionary encoded
     * and once as plain rows, and asserts both give the same values, the first as a dictionary when {@code reusesOrdinals}.
     */
    private void assertDictionaryPath(
        Function<Expression, Expression> function,
        String[] dictionary,
        Integer[] ordinals,
        boolean reusesOrdinals
    ) {
        Expression expression = function.apply(field("labels", DataType.KEYWORD));
        String[] rows = new String[ordinals.length];
        for (int p = 0; p < ordinals.length; p++) {
            rows[p] = ordinals[p] == null ? null : dictionary[ordinals[p]];
        }
        List<List<String>> encoded;
        try (Block result = evaluate(expression, ordinalBlock(dictionary, ordinals), rows.length)) {
            assertThat(result, reusesOrdinals ? instanceOf(OrdinalBytesRefBlock.class) : not(instanceOf(OrdinalBytesRefBlock.class)));
            encoded = values(result);
        }
        List<List<String>> plain;
        try (Block result = evaluate(expression, plainBlock(rows), rows.length)) {
            plain = values(result);
        }
        assertThat(encoded, equalTo(plain));
    }

    /** Evaluates {@code expression} over a page of {@code labels} and, when it reads one, a column {@code job} of row numbers. */
    private Block evaluate(Expression expression, BytesRefBlock labels, int positions) {
        DriverContext context = driverContext();
        Block job = null;
        try (BytesRefBlock.Builder builder = context.blockFactory().newBytesRefBlockBuilder(positions)) {
            for (int p = 0; p < positions; p++) {
                builder.appendBytesRef(new BytesRef("job-" + p));
            }
            job = builder.build();
        } finally {
            if (job == null) {
                labels.close();
            }
        }
        Page page = new Page(labels, job);
        try (ExpressionEvaluator evaluator = AbstractScalarFunctionTestCase.evaluator(expression).get(context)) {
            return evaluator.eval(page);
        } finally {
            page.releaseBlocks();
        }
    }

    private static List<List<String>> values(Block block) {
        BytesRefBlock bytes = (BytesRefBlock) block;
        BytesRef scratch = new BytesRef();
        List<List<String>> values = new ArrayList<>(block.getPositionCount());
        for (int p = 0; p < block.getPositionCount(); p++) {
            if (block.isNull(p)) {
                values.add(null);
                continue;
            }
            List<String> position = new ArrayList<>();
            for (int i = block.getFirstValueIndex(p); i < block.getFirstValueIndex(p) + block.getValueCount(p); i++) {
                position.add(bytes.getBytesRef(i, scratch).utf8ToString());
            }
            values.add(position);
        }
        return values;
    }

    private BytesRefBlock ordinalBlock(String[] dictionary, Integer[] ordinals) {
        BlockFactory factory = driverContext().blockFactory();
        BytesRefVector entries = null;
        IntBlock rows = null;
        try {
            try (BytesRefVector.Builder builder = factory.newBytesRefVectorBuilder(dictionary.length)) {
                for (String entry : dictionary) {
                    builder.appendBytesRef(new BytesRef(entry));
                }
                entries = builder.build();
            }
            try (IntBlock.Builder builder = factory.newIntBlockBuilder(ordinals.length)) {
                for (Integer ordinal : ordinals) {
                    if (ordinal == null) {
                        builder.appendNull();
                    } else {
                        builder.appendInt(ordinal);
                    }
                }
                rows = builder.build();
            }
            OrdinalBytesRefBlock result = new OrdinalBytesRefBlock(rows, entries);
            entries = null;
            rows = null;
            return result;
        } finally {
            Releasables.closeExpectNoException(entries, rows);
        }
    }

    private BytesRefBlock plainBlock(String[] rows) {
        try (BytesRefBlock.Builder builder = driverContext().blockFactory().newBytesRefBlockBuilder(rows.length)) {
            for (String row : rows) {
                if (row == null) {
                    builder.appendNull();
                } else {
                    builder.appendBytesRef(new BytesRef(row));
                }
            }
            return builder.build();
        }
    }

    private static Literal keyword(String value) {
        return new Literal(Source.EMPTY, new BytesRef(value), DataType.KEYWORD);
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
    public void allBreakersEmpty() {
        for (CircuitBreaker breaker : breakers) {
            assertThat("Breaker not empty: " + breaker.getName(), breaker.getUsed(), equalTo(0L));
        }
    }
}
