/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.timeseries;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase.evaluator;
import static org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase.field;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * A label container block holds each label set once and an ordinal per row: the functions run once per label set and keep
 * the ordinals, with the same answers as row by row. {@link ComputeTestCase} checks every block is released.
 */
public class TimeSeriesMetadataDictionaryTests extends ComputeTestCase {
    private static final BytesRef A = TimeSeriesMetadataEncoding.encode(Map.of("job", List.of("api"), "pod", List.of("a")));
    private static final BytesRef B = TimeSeriesMetadataEncoding.encode(Map.of("job", List.of("web"), "pod", List.of("b", "c")));
    /** A row per position: 0 and 1 point at {@link #A} and {@link #B}, null is a missing label container. */
    private static final Integer[] ROWS = { 0, 1, 0, null, 1, 0, 0, 1, null, 0, 1, 0 };

    public void testUnsetKeepsTheOrdinals() {
        List<BytesRef> expected = expected(
            TimeSeriesMetadataEncoding.encode(Map.of("job", List.of("api"))),
            TimeSeriesMetadataEncoding.encode(Map.of("job", List.of("web"))),
            TimeSeriesMetadataEncoding.EMPTY
        );
        assertRewrite(new TimeSeriesMetadataFilter(Source.EMPTY, labels(), List.of(keyword("pod")), true), expected);
    }

    public void testKeepKeepsTheOrdinals() {
        List<BytesRef> expected = expected(
            TimeSeriesMetadataEncoding.encode(Map.of("pod", List.of("a"))),
            TimeSeriesMetadataEncoding.encode(Map.of("pod", List.of("b", "c"))),
            TimeSeriesMetadataEncoding.EMPTY
        );
        assertRewrite(new TimeSeriesMetadataFilter(Source.EMPTY, labels(), List.of(keyword("pod")), false), expected);
    }

    /** A value that is the same on every row rewrites each label set once too. */
    public void testSetOfOneValueKeepsTheOrdinals() {
        List<BytesRef> expected = expected(
            TimeSeriesMetadataEncoding.encode(Map.of("env", List.of("prod"), "job", List.of("api"), "pod", List.of("a"))),
            TimeSeriesMetadataEncoding.encode(Map.of("env", List.of("prod"), "job", List.of("web"), "pod", List.of("b", "c"))),
            TimeSeriesMetadataEncoding.encode(Map.of("env", List.of("prod")))
        );
        assertRewrite(new TimeSeriesMetadataSet(Source.EMPTY, labels(), keyword("env"), keyword("prod")), expected);
    }

    /**
     * A value read off the same label set, as {@code label_replace} writes, is dictionary encoded too: each pair of a label set and a
     * value rewrites once, and the result stays dictionary encoded.
     */
    public void testSetOfAValueOfTheSameLabelSetKeepsTheOrdinals() {
        List<BytesRef> expected = expected(
            TimeSeriesMetadataEncoding.encode(Map.of("copy", List.of("api"), "job", List.of("api"), "pod", List.of("a"))),
            TimeSeriesMetadataEncoding.encode(Map.of("copy", List.of("web"), "job", List.of("web"), "pod", List.of("b", "c"))),
            TimeSeriesMetadataEncoding.EMPTY
        );
        Expression labels = labels();
        Expression job = new TimeSeriesMetadataPath(Source.EMPTY, labels, keyword("job"));
        assertRewrite(new TimeSeriesMetadataSet(Source.EMPTY, labels, keyword("copy"), job), expected);
    }

    /** A read keeps the ordinals when every label set holds one value of the label; a missing label reads as null. */
    public void testReadKeepsTheOrdinals() {
        try (Block result = eval(new TimeSeriesMetadataPath(Source.EMPTY, labels(), keyword("job")))) {
            BytesRefBlock values = (BytesRefBlock) result;
            assertThat(values.asOrdinals(), notNullValue());
            BytesRef scratch = new BytesRef();
            for (int p = 0; p < ROWS.length; p++) {
                if (ROWS[p] == null) {
                    assertTrue(values.isNull(p));
                } else {
                    String job = ROWS[p] == 0 ? "api" : "web";
                    assertThat(values.getBytesRef(values.getFirstValueIndex(p), scratch).utf8ToString(), equalTo(job));
                }
            }
        }
    }

    /** A label set holding several values of the label reads row by row: one entry cannot hold several values. */
    public void testReadOfSeveralValues() {
        try (Block result = eval(new TimeSeriesMetadataPath(Source.EMPTY, labels(), keyword("pod")))) {
            BytesRefBlock values = (BytesRefBlock) result;
            assertThat(values.asOrdinals(), nullValue());
            BytesRef scratch = new BytesRef();
            for (int p = 0; p < ROWS.length; p++) {
                if (ROWS[p] == null) {
                    assertTrue(values.isNull(p));
                    continue;
                }
                List<String> read = new ArrayList<>();
                for (int i = 0; i < values.getValueCount(p); i++) {
                    read.add(values.getBytesRef(values.getFirstValueIndex(p) + i, scratch).utf8ToString());
                }
                assertThat(read, equalTo(ROWS[p] == 0 ? List.of("a") : List.of("b", "c")));
            }
        }
    }

    private void assertRewrite(Expression function, List<BytesRef> expected) {
        try (Block result = eval(function)) {
            BytesRefBlock rewritten = (BytesRefBlock) result;
            assertThat(rewritten.asOrdinals(), notNullValue());
            BytesRef scratch = new BytesRef();
            for (int p = 0; p < ROWS.length; p++) {
                assertThat(rewritten.getBytesRef(rewritten.getFirstValueIndex(p), scratch), equalTo(expected.get(p)));
            }
        }
    }

    /** The rewrite expected at each row, from the rewrites of {@link #A}, {@link #B} and of a missing label container. */
    private static List<BytesRef> expected(BytesRef a, BytesRef b, BytesRef missing) {
        List<BytesRef> expected = new ArrayList<>();
        for (Integer row : ROWS) {
            expected.add(row == null ? missing : row == 0 ? a : b);
        }
        return expected;
    }

    private Block eval(Expression function) {
        BlockFactory blockFactory = blockFactory();
        DriverContext context = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        try (ExpressionEvaluator evaluator = evaluator(function).get(context); Page page = new Page(labelContainers(blockFactory))) {
            return evaluator.eval(page);
        }
    }

    private static OrdinalBytesRefBlock labelContainers(BlockFactory blockFactory) {
        IntBlock ordinals = null;
        BytesRefVector dictionary = null;
        try (
            IntBlock.Builder rows = blockFactory.newIntBlockBuilder(ROWS.length);
            BytesRefVector.Builder entries = blockFactory.newBytesRefVectorBuilder(2)
        ) {
            for (Integer row : ROWS) {
                if (row == null) {
                    rows.appendNull();
                } else {
                    rows.appendInt(row);
                }
            }
            entries.appendBytesRef(A);
            entries.appendBytesRef(B);
            ordinals = rows.build();
            dictionary = entries.build();
            OrdinalBytesRefBlock block = new OrdinalBytesRefBlock(ordinals, dictionary);
            assertTrue(block.isDense());
            ordinals = null;
            dictionary = null;
            return block;
        } finally {
            if (ordinals != null) {
                ordinals.close();
            }
            if (dictionary != null) {
                dictionary.close();
            }
        }
    }

    private static Expression labels() {
        return field("labels", DataType.TIME_SERIES_METADATA_DATA_TYPE);
    }

    private static Expression keyword(String value) {
        return Literal.keyword(Source.EMPTY, value);
    }
}
