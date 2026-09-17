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
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.junit.After;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Unit tests for the dictionary-memoization fast path in {@link ReplaceConstantOrdinalEvaluator}.
 * <p>
 * The {@link ReplaceTests}/{@link ReplaceStaticTests} suites already cover correctness on plain
 * {@link BytesRefBlock} inputs; this class focuses on the ordinal path:
 * <ul>
 *   <li>An {@link OrdinalBytesRefBlock} input that is dense and single-valued takes the fast path
 *       and produces an {@link OrdinalBytesRefBlock} with a transformed dictionary.</li>
 *   <li>Output values match the per-row reference (the input fed through plain {@code String.replaceAll}).</li>
 *   <li>Nulls in the ordinal block are preserved at the same positions.</li>
 *   <li>A non-dense ordinal block falls back to per-row evaluation but still produces the right values.</li>
 *   <li>If a dictionary entry would overflow {@code MAX_BYTES_REF_RESULT_SIZE}, the evaluator falls
 *       back to per-row evaluation and emits the same warning shape as the legacy path.</li>
 * </ul>
 * <p>
 * The {@code testPerRow*} methods below also cover the page-scoped memoization cache in the per-row
 * fallback, using a plain (non-ordinal) {@link BytesRefBlock} input.
 */
public class ReplaceOrdinalTests extends ESTestCase {

    private final List<CircuitBreaker> breakers = Collections.synchronizedList(new ArrayList<>());
    private final List<DriverContext> driverContexts = Collections.synchronizedList(new ArrayList<>());

    /**
     * Asserts the warnings accumulated across every {@link DriverContext} this test built.
     */
    private void assertDriverWarnings(String... expected) {
        Set<String> collected = new LinkedHashSet<>();
        for (DriverContext ctx : driverContexts) {
            if (ctx.isFinished() == false) {
                ctx.finish();
            }
            collected.addAll(ctx.warnings());
        }
        assertThat(collected, containsInAnyOrder(expected));
    }

    public void testDictionaryFastPathProducesOrdinalBlock() {
        String[] dictionary = { "http://a.example.com/x", "https://b.example.com/y", "ftp://c.example.org/z" };
        int[] ordinals = { 0, 1, 0, 2, 1, 0, 1, 2, 0, 0, 1, 2, 2, 0, 1 }; // 15 rows
        try (Block result = runReplace(dictionary, ordinals, "^https?://(?:www\\.)?([^/]+)/.*$", "$1")) {
            assertThat("dense ordinal input must produce an OrdinalBytesRefBlock", result, instanceOf(OrdinalBytesRefBlock.class));
            assertResultValues(result, dictionary, ordinals, "^https?://(?:www\\.)?([^/]+)/.*$", "$1");
        }
    }

    public void testDictionaryFastPathPreservesNullPositions() {
        String[] dictionary = { "http://a/", "http://b/", "http://c/" };
        // Use null in the ordinals at positions 2 and 5. Ordinals are dense (15 rows, 3 dict entries → ratio 5).
        Integer[] ordinals = { 0, 1, null, 2, 0, null, 1, 2, 0, 1, 2, 0, 1, 2, 0 };
        try (Block result = runReplaceWithNulls(dictionary, ordinals, "^https?://([^/]+)/.*$", "$1")) {
            assertThat(result, instanceOf(OrdinalBytesRefBlock.class));
            for (int p = 0; p < ordinals.length; p++) {
                if (ordinals[p] == null) {
                    assertTrue("position " + p + " should be null", result.isNull(p));
                } else {
                    assertFalse("position " + p + " should not be null", result.isNull(p));
                }
            }
        }
    }

    public void testSparseOrdinalFallsBackToPerRow() {
        // isDense() requires totalPositions >= 10 AND totalPositions >= 2 * dictSize. A 3-row block with a
        // 3-entry dictionary fails both, so the evaluator must fall back to the per-row path.
        String[] dictionary = { "http://a/", "https://b/", "ftp://c/" };
        int[] ordinals = { 0, 1, 2 };
        try (Block result = runReplace(dictionary, ordinals, "^https?://([^/]+)/.*$", "$1")) {
            // The per-row path produces a regular BytesRefBlock, not an OrdinalBytesRefBlock.
            assertThat(result, instanceOf(BytesRefBlock.class));
            assertResultValues(result, dictionary, ordinals, "^https?://([^/]+)/.*$", "$1");
        }
    }

    public void testDictionaryEntryOverflowFallsBackToPerRow() {
        // Build a dictionary where one entry, when replaced, exceeds MAX_BYTES_REF_RESULT_SIZE. The
        // dictionary path should bail and the per-row path should produce null + a warning for the affected
        // rows (and correct values for the rest).
        int oversizeLen = (int) (ScalarFunction.MAX_BYTES_REF_RESULT_SIZE / 10);
        String oversize = "a".repeat(oversizeLen);
        // Regex `.` with replacement equal to the entry itself; one match expands 1 byte to oversizeLen bytes.
        String regex = ".";
        String newStr = oversize;

        String[] dictionary = { "abc", oversize }; // entry 0 small, entry 1 oversize
        // 12 rows alternating: 0,1,0,1,0,1,0,1,0,1,0,1 — dense + 2*dictSize=4 ≤ 12. Triggers fast-path attempt
        // which must bail when entry 1 overflows.
        int[] ordinals = { 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1 };
        try (Block result = runReplace(dictionary, ordinals, regex, newStr)) {
            // Fallback path → plain BytesRefBlock with nulls at the oversize-entry positions.
            for (int p = 0; p < ordinals.length; p++) {
                if (ordinals[p] == 1) {
                    assertTrue("position " + p + " (oversize entry) must be null", result.isNull(p));
                } else {
                    assertFalse("position " + p + " (small entry) must not be null", result.isNull(p));
                }
            }
            assertDriverWarnings(
                "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                "Line -1:-1: java.lang.IllegalArgumentException: "
                    + "Creating strings with more than ["
                    + ScalarFunction.MAX_BYTES_REF_RESULT_SIZE
                    + "] bytes is not supported"
            );
        }
    }

    /** Plain (non-ordinal) input with heavy repetition must match the plain reference row-for-row. */
    public void testPerRowMemoizationMatchesReferenceOnPlainBlock() {
        String regex = "^https?://(?:www\\.)?([^/]+)/.*$";
        String newStr = "$1";
        String[] pool = {
            "https://www.example.com/path?x=1",
            "http://other.example.org/a/b/c",
            "https://third.example.net/",
            "not-a-url-at-all",
            "https://singleton.example.io/only/once" };
        int[] rowToPool = { 0, 0, 0, 0, 1, 2, 3, 1, 3, 4, 0, 2 };

        try (Block result = runReplacePlainBlock(pool, rowToPool, regex, newStr)) {
            assertThat(result, instanceOf(BytesRefBlock.class));
            BytesRefBlock bb = (BytesRefBlock) result;
            BytesRef scratch = new BytesRef();
            for (int p = 0; p < rowToPool.length; p++) {
                String expected = pool[rowToPool[p]].replaceAll(regex, newStr);
                assertThat("row " + p, bb.getBytesRef(p, scratch).utf8ToString(), equalTo(expected));
            }
        }
    }

    /** A repeated failing value must fail independently on every occurrence, not just the first. */
    public void testPerRowMemoizationDoesNotSuppressRepeatedFailures() {
        int oversizeLen = (int) (ScalarFunction.MAX_BYTES_REF_RESULT_SIZE / 10);
        String oversize = "a".repeat(oversizeLen);
        String regex = ".";
        String newStr = oversize;
        String[] pool = { "abc", oversize };
        int[] rowToPool = { 1, 0, 1, 0, 1, 0 }; // oversize value, non-adjacent, 3 times

        try (Block result = runReplacePlainBlock(pool, rowToPool, regex, newStr)) {
            for (int p = 0; p < rowToPool.length; p++) {
                if (rowToPool[p] == 1) {
                    assertTrue("row " + p + " (oversize) must be null", result.isNull(p));
                } else {
                    assertFalse("row " + p + " (small) must not be null", result.isNull(p));
                }
            }
            assertDriverWarnings(
                "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                "Line -1:-1: java.lang.IllegalArgumentException: "
                    + "Creating strings with more than ["
                    + ScalarFunction.MAX_BYTES_REF_RESULT_SIZE
                    + "] bytes is not supported"
            );
        }
    }

    /** Nulls and multi-value rows must bypass the cache, same as the legacy per-row path. */
    public void testPerRowMemoizationHandlesNullsAndMultivalues() {
        String regex = "^https?://([^/]+)/.*$";
        String newStr = "$1";
        try (
            Block result = runReplacePlainBlockWithNullsAndMultivalues(
                new String[] { "https://a.example.com/x", null, "https://a.example.com/x", MULTIVALUE_SENTINEL, "https://a.example.com/x" },
                regex,
                newStr
            )
        ) {
            BytesRefBlock bb = (BytesRefBlock) result;
            BytesRef scratch = new BytesRef();
            assertThat(bb.getBytesRef(0, scratch).utf8ToString(), equalTo("a.example.com"));
            assertTrue("null input row must stay null", result.isNull(1));
            assertThat(bb.getBytesRef(2, scratch).utf8ToString(), equalTo("a.example.com"));
            assertTrue("multi-value row must be null with a warning", result.isNull(3));
            assertThat(bb.getBytesRef(4, scratch).utf8ToString(), equalTo("a.example.com"));
            assertDriverWarnings(
                "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                "Line -1:-1: java.lang.IllegalArgumentException: single-value function encountered multi-value"
            );
        }
    }

    /** A value recurring across a null, multi-value, or failing gap must still produce the right result. */
    public void testPerRowMemoizationReusesLastSuccessAcrossGaps() {
        String regex = "^https?://([^/]+)/.*$";
        String newStr = "$1";
        int oversizeLen = (int) (ScalarFunction.MAX_BYTES_REF_RESULT_SIZE / 10);
        String oversizeRegex = ".";
        String oversizeNewStr = "a".repeat(oversizeLen);

        // rows 0/2: same value across a null gap; rows 3/5: same value across a multi-value gap
        try (
            Block result = runReplacePlainBlockWithNullsAndMultivalues(
                new String[] {
                    "https://a.example.com/x",
                    null,
                    "https://a.example.com/x",
                    "https://b.example.com/y",
                    MULTIVALUE_SENTINEL,
                    "https://b.example.com/y" },
                regex,
                newStr
            )
        ) {
            BytesRefBlock bb = (BytesRefBlock) result;
            BytesRef scratch = new BytesRef();
            assertThat(bb.getBytesRef(0, scratch).utf8ToString(), equalTo("a.example.com"));
            assertTrue(result.isNull(1));
            assertThat("value recurring across a null gap", bb.getBytesRef(2, scratch).utf8ToString(), equalTo("a.example.com"));
            assertThat(bb.getBytesRef(3, scratch).utf8ToString(), equalTo("b.example.com"));
            assertTrue(result.isNull(4));
            assertThat("value recurring across a multi-value gap", bb.getBytesRef(5, scratch).utf8ToString(), equalTo("b.example.com"));
        }

        // row 0 succeeds, row 1 fails, row 2 repeats row 0, row 3 repeats row 1's failing value
        try (
            Block result = runReplacePlainBlock(new String[] { "abc", oversizeNewStr }, new int[] { 0, 1, 0, 1 }, oversizeRegex, oversizeNewStr)
        ) {
            BytesRefBlock bb = (BytesRefBlock) result;
            BytesRef scratch = new BytesRef();
            String expectedSuccess = "abc".replaceAll(oversizeRegex, oversizeNewStr);
            assertThat("row 0 (succeeds)", bb.getBytesRef(0, scratch).utf8ToString(), equalTo(expectedSuccess));
            assertTrue("row 1 (oversize, fails)", result.isNull(1));
            assertThat(
                "row 2 (repeat of row 0's value across the failure)",
                bb.getBytesRef(2, scratch).utf8ToString(),
                equalTo(expectedSuccess)
            );
            assertTrue("row 3 (repeat of row 1's failing value)", result.isNull(3));
        }
    }

    private static final String MULTIVALUE_SENTINEL = "__MULTIVALUE__";

    private Block runReplace(String[] dictionary, int[] ordinals, String regex, String newStr) {
        Integer[] boxed = new Integer[ordinals.length];
        for (int i = 0; i < ordinals.length; i++) {
            boxed[i] = ordinals[i];
        }
        return runReplaceWithNulls(dictionary, boxed, regex, newStr);
    }

    private Block runReplaceWithNulls(String[] dictionary, Integer[] ordinals, String regex, String newStr) {
        DriverContext ctx = driverContext();
        OrdinalBytesRefBlock strBlock = buildOrdinalBlock(ctx.blockFactory(), dictionary, ordinals);
        ExpressionEvaluator.Factory factory = AbstractScalarFunctionTestCase.evaluator(
            new Replace(
                Source.EMPTY,
                field("text", DataType.KEYWORD),
                new Literal(Source.EMPTY, new BytesRef(regex), DataType.KEYWORD),
                new Literal(Source.EMPTY, new BytesRef(newStr), DataType.KEYWORD)
            )
        );
        try (ExpressionEvaluator eval = factory.get(ctx)) {
            Page page = new Page(strBlock);
            try {
                return eval.eval(page);
            } finally {
                page.releaseBlocks();
            }
        }
    }

    /** Builds a plain {@link BytesRefBlock} (never an {@link OrdinalBytesRefBlock}), forcing the per-row path. */
    private Block runReplacePlainBlock(String[] pool, int[] rowToPool, String regex, String newStr) {
        String[] values = new String[rowToPool.length];
        for (int i = 0; i < rowToPool.length; i++) {
            values[i] = pool[rowToPool[i]];
        }
        return runReplacePlainBlockValues(values, regex, newStr);
    }

    private Block runReplacePlainBlockWithNullsAndMultivalues(String[] values, String regex, String newStr) {
        return runReplacePlainBlockValues(values, regex, newStr);
    }

    private Block runReplacePlainBlockValues(String[] values, String regex, String newStr) {
        DriverContext ctx = driverContext();
        BytesRefBlock strBlock = null;
        try (BytesRefBlock.Builder builder = ctx.blockFactory().newBytesRefBlockBuilder(values.length)) {
            for (String value : values) {
                if (value == null) {
                    builder.appendNull();
                } else if (value.equals(MULTIVALUE_SENTINEL)) {
                    builder.beginPositionEntry();
                    builder.appendBytesRef(new BytesRef("a"));
                    builder.appendBytesRef(new BytesRef("b"));
                    builder.endPositionEntry();
                } else {
                    builder.appendBytesRef(new BytesRef(value));
                }
            }
            strBlock = builder.build();
        }
        assertThat("test helper must build a plain (non-ordinal) block", strBlock.asOrdinals(), equalTo(null));

        ExpressionEvaluator.Factory factory = AbstractScalarFunctionTestCase.evaluator(
            new Replace(
                Source.EMPTY,
                field("text", DataType.KEYWORD),
                new Literal(Source.EMPTY, new BytesRef(regex), DataType.KEYWORD),
                new Literal(Source.EMPTY, new BytesRef(newStr), DataType.KEYWORD)
            )
        );
        try (ExpressionEvaluator eval = factory.get(ctx)) {
            Page page = new Page(strBlock);
            try {
                return eval.eval(page);
            } finally {
                page.releaseBlocks();
            }
        }
    }

    private static OrdinalBytesRefBlock buildOrdinalBlock(BlockFactory blockFactory, String[] dictionary, Integer[] ordinals) {
        BytesRefVector dictVector = null;
        IntBlock ordinalsBlock = null;
        try (BytesRefVector.Builder dictBuilder = blockFactory.newBytesRefVectorBuilder(dictionary.length)) {
            for (String entry : dictionary) {
                dictBuilder.appendBytesRef(new BytesRef(entry));
            }
            dictVector = dictBuilder.build();
            try (IntBlock.Builder b = blockFactory.newIntBlockBuilder(ordinals.length)) {
                for (Integer ord : ordinals) {
                    if (ord == null) {
                        b.appendNull();
                    } else {
                        b.appendInt(ord);
                    }
                }
                ordinalsBlock = b.build();
            }
            OrdinalBytesRefBlock result = new OrdinalBytesRefBlock(ordinalsBlock, dictVector);
            dictVector = null;
            ordinalsBlock = null;
            return result;
        } finally {
            if (dictVector != null) {
                dictVector.close();
            }
            if (ordinalsBlock != null) {
                ordinalsBlock.close();
            }
        }
    }

    private static void assertResultValues(Block result, String[] dictionary, int[] ordinals, String regex, String newStr) {
        BytesRefBlock bb = (BytesRefBlock) result;
        BytesRef scratch = new BytesRef();
        for (int p = 0; p < ordinals.length; p++) {
            String expected = dictionary[ordinals[p]].replaceAll(regex, newStr);
            BytesRef actual = bb.getBytesRef(p, scratch);
            assertThat("row " + p + " (ordinal " + ordinals[p] + ")", actual.utf8ToString(), equalTo(expected));
        }
    }

    private static FieldAttribute field(String name, DataType type) {
        return new FieldAttribute(Source.synthetic(name), name, new EsField(name, type, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
    }

    private DriverContext driverContext() {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(256)).withCircuitBreaking();
        breakers.add(bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST));
        DriverContext driverContext = new DriverContext(bigArrays, BlockFactory.builder(bigArrays).build(), null);
        driverContexts.add(driverContext);
        return driverContext;
    }

    @After
    public void allMemoryReleased() {
        for (CircuitBreaker breaker : breakers) {
            assertThat(breaker.getUsed(), equalTo(0L));
        }
    }

}
