/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.promql.function;

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
import static org.hamcrest.Matchers.instanceOf;

/**
 * Unit tests for the dictionary fast path in {@link RegexExpandOrdinalEvaluator}. {@link RegexExpandTests} already covers the
 * match/expand semantics on plain blocks; this suite focuses on the ordinal path and its equivalence to the per-row path:
 * <ul>
 *   <li>a dense, single-valued {@link OrdinalBytesRefBlock} whose entries all match takes the fast path and produces an
 *       {@link OrdinalBytesRefBlock} (reused ordinals, rebuilt dictionary);</li>
 *   <li>when a dictionary entry does not match (the no-op {@code null} sentinel, which cannot live in a dictionary), the
 *       evaluator materializes a plain {@link BytesRefBlock};</li>
 *   <li>null positions and the empty (delete) sentinel are preserved; a non-dense ordinal block falls back to per-row;</li>
 *   <li>every case is asserted to be byte-for-byte identical to feeding the same logical values through the per-row path.</li>
 * </ul>
 */
public class RegexExpandOrdinalTests extends ESTestCase {

    private final List<CircuitBreaker> breakers = Collections.synchronizedList(new ArrayList<>());

    public void testDictionaryFastPathProducesOrdinalBlock() {
        String[] dictionary = { "source-value-a", "source-value-b", "source-value-c" };
        Integer[] ordinals = { 0, 1, 0, 2, 1, 0, 1, 2, 0, 0, 1, 2, 2, 0, 1 }; // 15 rows, dictSize 3 -> dense
        assertFastPathMatchesPerRow(dictionary, ordinals, "source-value-(.*)", "$1", true);
    }

    public void testNoMatchEntryMaterializesPlainBlock() {
        // The middle entry does not match the anchored pattern, so its expansion is the no-op null sentinel. That cannot be
        // represented in a dictionary vector, so the fast path materializes a plain BytesRefBlock with nulls at those rows.
        String[] dictionary = { "source-value-a", "does-not-match", "source-value-c" };
        Integer[] ordinals = { 0, 1, 0, 2, 1, 0, 1, 2, 0, 0, 1, 2, 2, 0, 1 };
        assertFastPathMatchesPerRow(dictionary, ordinals, "source-value-(.*)", "$1", false);
    }

    public void testDictionaryFastPathPreservesNullPositions() {
        // All entries match, so the ordinals are reused; the nulls carried in the ordinals block must survive unchanged.
        String[] dictionary = { "source-value-a", "source-value-b", "source-value-c" };
        Integer[] ordinals = { 0, 1, null, 2, 0, null, 1, 2, 0, 1, 2, 0, 1, 2, 0 };
        assertFastPathMatchesPerRow(dictionary, ordinals, "source-value-(.*)", "$1", true);
    }

    public void testEmptyExpansionDeleteSentinelReusesOrdinals() {
        // A match whose expansion is empty is the delete sentinel (empty BytesRef, not null), which a dictionary can hold, so
        // the ordinals are still reused. Every row must be non-null and equal to the empty string.
        String[] dictionary = { "a", "bb", "ccc" };
        Integer[] ordinals = { 0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 1, 2 };
        assertFastPathMatchesPerRow(dictionary, ordinals, ".*", "", true);
    }

    public void testSparseOrdinalFallsBackToPerRow() {
        // isDense() requires totalPositions >= 10 and >= 2 * dictSize; a 3-row block with a 3-entry dictionary fails both, so
        // the evaluator takes the per-row path and returns a plain BytesRefBlock.
        String[] dictionary = { "source-value-a", "source-value-b", "source-value-c" };
        Integer[] ordinals = { 0, 1, 2 };
        assertFastPathMatchesPerRow(dictionary, ordinals, "source-value-(.*)", "$1", false);
    }

    /**
     * Runs the ordinal input through the evaluator and asserts the result is (or is not) an {@link OrdinalBytesRefBlock}, then
     * asserts it is position-for-position identical to feeding the same logical values through the per-row path.
     */
    private void assertFastPathMatchesPerRow(
        String[] dictionary,
        Integer[] ordinals,
        String regex,
        String replacement,
        boolean expectOrdinalBlock
    ) {
        List<String> fastPath;
        try (Block result = runOrdinal(dictionary, ordinals, regex, replacement)) {
            if (expectOrdinalBlock) {
                assertThat(result, instanceOf(OrdinalBytesRefBlock.class));
            } else {
                assertFalse("expected a materialized plain block", result instanceof OrdinalBytesRefBlock);
            }
            fastPath = extract(result, ordinals.length);
        }

        String[] values = new String[ordinals.length];
        for (int p = 0; p < ordinals.length; p++) {
            values[p] = ordinals[p] == null ? null : dictionary[ordinals[p]];
        }
        List<String> perRow;
        try (Block result = runPlain(values, regex, replacement)) {
            assertFalse("per-row path must produce a plain block", result instanceof OrdinalBytesRefBlock);
            perRow = extract(result, ordinals.length);
        }

        assertThat(fastPath, equalTo(perRow));
    }

    private Block runOrdinal(String[] dictionary, Integer[] ordinals, String regex, String replacement) {
        DriverContext ctx = driverContext();
        OrdinalBytesRefBlock srcBlock = buildOrdinalBlock(ctx.blockFactory(), dictionary, ordinals);
        ExpressionEvaluator.Factory factory = AbstractScalarFunctionTestCase.evaluator(function(regex, replacement));
        try (ExpressionEvaluator eval = factory.get(ctx)) {
            Page page = new Page(srcBlock);
            try {
                return eval.eval(page);
            } finally {
                page.releaseBlocks();
            }
        }
    }

    private Block runPlain(String[] values, String regex, String replacement) {
        DriverContext ctx = driverContext();
        BytesRefBlock srcBlock;
        try (BytesRefBlock.Builder builder = ctx.blockFactory().newBytesRefBlockBuilder(values.length)) {
            for (String value : values) {
                if (value == null) {
                    builder.appendNull();
                } else {
                    builder.appendBytesRef(new BytesRef(value));
                }
            }
            srcBlock = builder.build();
        }
        ExpressionEvaluator.Factory factory = AbstractScalarFunctionTestCase.evaluator(function(regex, replacement));
        try (ExpressionEvaluator eval = factory.get(ctx)) {
            Page page = new Page(srcBlock);
            try {
                return eval.eval(page);
            } finally {
                page.releaseBlocks();
            }
        }
    }

    private static List<String> extract(Block block, int positionCount) {
        BytesRefBlock bytesRefBlock = (BytesRefBlock) block;
        BytesRef scratch = new BytesRef();
        List<String> values = new ArrayList<>(positionCount);
        for (int p = 0; p < positionCount; p++) {
            values.add(block.isNull(p) ? null : bytesRefBlock.getBytesRef(bytesRefBlock.getFirstValueIndex(p), scratch).utf8ToString());
        }
        return values;
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

    private static RegexExpand function(String regex, String replacement) {
        Source source = Source.EMPTY;
        return new RegexExpand(
            source,
            field("src", DataType.KEYWORD),
            new Literal(source, new BytesRef(regex), DataType.KEYWORD),
            new Literal(source, new BytesRef(replacement), DataType.KEYWORD)
        );
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
