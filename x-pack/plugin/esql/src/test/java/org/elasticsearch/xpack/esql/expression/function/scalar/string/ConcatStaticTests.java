/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlClientException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.junit.After;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.IntSupplier;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 * Tests for {@link Concat} with values far longer than the ones in {@link ConcatTests}. None of them
 * uses a test case, and {@link ConcatTests} would run each of them once for every test case it has.
 */
public class ConcatStaticTests extends ESTestCase {
    private static final String TOO_LONG = "concatenating more than [" + Concat.MAX_CONCAT_LENGTH + "] bytes is not supported";

    /**
     * Values long enough to span several pages of their block, in a page of several rows so the blocks are backed
     * by arrays rather than constants. Each argument has to be copied whole, page by page, into the result.
     */
    public void testValuesSpanningPages() {
        assertConcatenates(between(2, 5), between(2, 4), () -> randomBoolean() ? between(0, 10) : between(20_000, 60_000));
    }

    /**
     * Values longer than 1MB, which used to be the limit, in a page of several rows.
     */
    public void testValuesOverOneMb() {
        assertConcatenates(between(2, 3), between(2, 3), () -> between(600_000, 2_000_000));
    }

    /**
     * A value of exactly the limit is built, and one byte more is refused.
     */
    public void testLimit() {
        byte[][] parts = new byte[between(2, 5)][];
        int remaining = Math.toIntExact(Concat.MAX_CONCAT_LENGTH);
        for (int a = 0; a < parts.length; a++) {
            parts[a] = randomAscii(a == parts.length - 1 ? remaining : between(0, remaining));
            remaining -= parts[a].length;
        }
        try (Block result = concatRow(driverContext(), parts)) {
            assertThat(toJavaObject(result, 0), equalTo(concatenate(parts)));
        }

        int longer = between(0, parts.length - 1);
        parts[longer] = randomAscii(parts[longer].length + 1);
        Exception e = expectThrows(EsqlClientException.class, () -> concatRow(driverContext(), parts).close());
        assertThat(e.getMessage(), is(TOO_LONG));
    }

    /**
     * A value over the limit is refused from the lengths of the arguments alone, before any of it is copied.
     * The breaker here has far less room than the value needs, and it is never asked for any.
     */
    public void testOversizedValueIsRefusedWithoutCopying() {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(1)).withCircuitBreaking();
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        DriverContext context = new DriverContext(bigArrays, BlockFactory.builder(bigArrays).build(), null);
        byte[][] parts = new byte[between(2, 20)][];
        for (int a = 0; a < parts.length; a++) {
            parts[a] = new byte[Math.toIntExact(Concat.MAX_CONCAT_LENGTH / parts.length) + 1];
        }
        Exception e = expectThrows(EsqlClientException.class, () -> concatRow(context, parts).close());
        assertThat(e.getMessage(), is(TOO_LONG));
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /**
     * Concatenates array backed blocks of {@code positions} rows and checks every row of the result.
     */
    private void assertConcatenates(int positions, int arguments, IntSupplier length) {
        DriverContext context = driverContext();
        Block[] blocks = new Block[arguments];
        BytesRefBuilder[] expected = new BytesRefBuilder[positions];
        for (int p = 0; p < positions; p++) {
            expected[p] = new BytesRefBuilder();
        }
        try {
            for (int a = 0; a < arguments; a++) {
                try (BytesRefBlock.Builder builder = context.blockFactory().newBytesRefBlockBuilder(positions)) {
                    for (int p = 0; p < positions; p++) {
                        byte[] value = randomAscii(length.getAsInt());
                        builder.appendBytesRef(new BytesRef(value));
                        expected[p].append(value, 0, value.length);
                    }
                    blocks[a] = builder.build();
                }
            }
            try (Block result = concat(context, new Page(positions, blocks))) {
                for (int p = 0; p < positions; p++) {
                    assertThat(toJavaObject(result, p), equalTo(expected[p].get()));
                }
            }
        } finally {
            Releasables.closeExpectNoException(blocks);
        }
    }

    /**
     * Concatenates a single row whose arguments are constants.
     */
    private static Block concatRow(DriverContext context, byte[][] parts) {
        Block[] blocks = new Block[parts.length];
        try {
            for (int a = 0; a < parts.length; a++) {
                blocks[a] = TestBlockFactory.getNonBreakingInstance().newConstantBytesRefBlockWith(new BytesRef(parts[a]), 1);
            }
            return concat(context, new Page(1, blocks));
        } finally {
            Releasables.closeExpectNoException(blocks);
        }
    }

    private static Block concat(DriverContext context, Page page) {
        List<Expression> fields = new ArrayList<>();
        for (int a = 0; a < page.getBlockCount(); a++) {
            fields.add(field("f" + a, randomFrom(DataType.KEYWORD, DataType.TEXT)));
        }
        Expression concat = new Concat(Source.EMPTY, fields.get(0), fields.subList(1, fields.size()));
        try (ExpressionEvaluator eval = AbstractScalarFunctionTestCase.evaluator(concat).get(context)) {
            return eval.eval(page);
        }
    }

    private static BytesRef concatenate(byte[][] parts) {
        BytesRefBuilder concatenated = new BytesRefBuilder();
        for (byte[] part : parts) {
            concatenated.append(part, 0, part.length);
        }
        return concatenated.get();
    }

    /**
     * Random lower case letters. Much faster for long values than drawing a random string.
     */
    private static byte[] randomAscii(int length) {
        byte[] bytes = randomByteArrayOfLength(length);
        for (int i = 0; i < length; i++) {
            bytes[i] = (byte) ('a' + (bytes[i] & 0x0f));
        }
        return bytes;
    }

    /**
     * The following fields and methods were borrowed from AbstractScalarFunctionTestCase
     */
    private final List<CircuitBreaker> breakers = Collections.synchronizedList(new ArrayList<>());

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
