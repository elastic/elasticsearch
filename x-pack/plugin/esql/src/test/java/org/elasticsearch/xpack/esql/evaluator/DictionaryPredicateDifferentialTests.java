/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.evaluator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.lucene.EmptyIndexedByShardId;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RLikePattern;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardPattern;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.EndsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.StartsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.ToLower;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.ToUpper;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.RLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.WildcardLike;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.InsensitiveEquals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.planner.Layout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

/**
 * Oracle: the unwrapped factory built through {@link EvalMapper#toEvaluatorContext(Layout, EvaluatorMapper.ToEvaluator)},
 * which skips dictionary wrapping. Compare row-by-row, including nulls, on mixed page shapes.
 */
public class DictionaryPredicateDifferentialTests extends ESTestCase {
    private static final BytesRef[] VOCAB = new BytesRef[] {
        new BytesRef("foo"),
        new BytesRef("bar"),
        new BytesRef("baz"),
        new BytesRef("qux"),
        new BytesRef("-"),
        new BytesRef("y"),
        new BytesRef(""),
        new BytesRef("日本語"),
        new BytesRef("x".repeat(256)) };

    public void testRandomExpressionsMatchUnwrapped() {
        FieldAttribute attr = field("a");
        Layout layout = layout(attr);
        EvaluatorMapper.ToEvaluator parent = EvalMapper.toEvaluatorContext(
            FoldContext.small(),
            layout,
            EmptyIndexedByShardId.instance(),
            null
        );
        BlockFactory factory = TestBlockFactory.getNonBreakingInstance();

        for (int iter = 0; iter < 20; iter++) {
            Expression exp = randomExpression(attr, 0);
            ExpressionEvaluator.Factory wrapped = EvalMapper.toEvaluator(FoldContext.small(), exp, layout);
            ExpressionEvaluator.Factory oracle = ((EvaluatorMapper) exp).toEvaluator(EvalMapper.toEvaluatorContext(layout, parent));
            DriverContext wrappedCtx = new DriverContext(factory.bigArrays(), factory, null);
            DriverContext oracleCtx = new DriverContext(factory.bigArrays(), factory, null);
            try (ExpressionEvaluator w = wrapped.get(wrappedCtx); ExpressionEvaluator o = oracle.get(oracleCtx)) {
                List<Page> pages = randomPages(factory);
                try {
                    for (Page page : pages) {
                        try (BooleanBlock wr = (BooleanBlock) w.eval(page); BooleanBlock or = (BooleanBlock) o.eval(page)) {
                            assertBlocksEqual(wr, or, page.getPositionCount());
                        }
                    }
                    wrappedCtx.finish();
                    oracleCtx.finish();
                    assertThat(wrappedCtx.warnings(), equalTo(oracleCtx.warnings()));
                } finally {
                    Releasables.close(pages.toArray(Page[]::new));
                }
            }
        }
    }

    public void testCranky() {
        FieldAttribute attr = field("a");
        Layout layout = layout(attr);
        Expression exp = new Equals(Source.EMPTY, attr, Literal.keyword(Source.EMPTY, "foo"));
        ExpressionEvaluator.Factory wrapped = EvalMapper.toEvaluator(FoldContext.small(), exp, layout);
        testWithCranky(wrapped);
    }

    private void testWithCranky(ExpressionEvaluator.Factory wrapped) {
        CrankyCircuitBreakerService cranky = new CrankyCircuitBreakerService();
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, cranky).withCircuitBreaking();
        BlockFactory factory = BlockFactory.builder(bigArrays).build();
        DriverContext ctx = new DriverContext(bigArrays, factory, null);
        try {
            try (ExpressionEvaluator eval = wrapped.get(ctx); Page page = denseOrdinals(factory, 64, 4, false)) {
                eval.eval(page).close();
            }
        } catch (CircuitBreakingException e) {
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
        assertThat(factory.breaker().getUsed(), equalTo(0L));
        assertThat(bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST).getUsed(), equalTo(0L));
    }

    private Expression randomExpression(FieldAttribute attr, int depth) {
        if (depth >= 3 || randomBoolean()) {
            return randomLeaf(attr);
        }
        return switch (randomIntBetween(0, 5)) {
            case 0 -> new And(Source.EMPTY, randomExpression(attr, depth + 1), randomExpression(attr, depth + 1));
            case 1 -> new Or(Source.EMPTY, randomExpression(attr, depth + 1), randomExpression(attr, depth + 1));
            case 2 -> new Not(Source.EMPTY, randomExpression(attr, depth + 1));
            case 3 -> new Or(Source.EMPTY, new IsNull(Source.EMPTY, attr), randomLeaf(attr));
            case 4 -> new And(Source.EMPTY, new IsNotNull(Source.EMPTY, attr), randomLeaf(attr));
            default -> randomLeaf(attr);
        };
    }

    private Expression randomLeaf(FieldAttribute attr) {
        BytesRef v = randomFrom(VOCAB);
        Expression field = switch (randomIntBetween(0, 2)) {
            case 0 -> attr;
            case 1 -> new ToLower(Source.EMPTY, attr, EsqlTestUtils.TEST_CFG);
            default -> new ToUpper(Source.EMPTY, attr, EsqlTestUtils.TEST_CFG);
        };
        return switch (randomIntBetween(0, 7)) {
            case 0 -> new Equals(Source.EMPTY, field, Literal.keyword(Source.EMPTY, v.utf8ToString()));
            case 1 -> new NotEquals(Source.EMPTY, field, Literal.keyword(Source.EMPTY, v.utf8ToString()));
            case 2 -> new StartsWith(Source.EMPTY, field, Literal.keyword(Source.EMPTY, "f"));
            case 3 -> new EndsWith(Source.EMPTY, field, Literal.keyword(Source.EMPTY, "o"));
            case 4 -> new WildcardLike(Source.EMPTY, field, new WildcardPattern("f*"));
            case 5 -> new RLike(Source.EMPTY, field, new RLikePattern("f.*"));
            case 6 -> new InsensitiveEquals(Source.EMPTY, field, Literal.keyword(Source.EMPTY, v.utf8ToString()));
            default -> new In(
                Source.EMPTY,
                field,
                List.of(Literal.keyword(Source.EMPTY, "foo"), Literal.keyword(Source.EMPTY, v.utf8ToString()))
            );
        };
    }

    private List<Page> randomPages(BlockFactory factory) {
        List<Page> pages = new ArrayList<>();
        pages.add(flat(factory, 32));
        pages.add(denseOrdinals(factory, 64, 4, false));
        pages.add(denseOrdinals(factory, 48, VOCAB.length, false));
        pages.add(paddedOrdinals(factory, 48, 200, 4));
        pages.add(paddedOrdinals(factory, 32, 10_000, 4));
        pages.add(denseOrdinals(factory, 40, 4, true));
        pages.add(allNullOrdinals(factory, 32));
        pages.add(denseOrdinals(factory, 5, 2, false));
        pages.add(mvOrdinals(factory, 24, 4));
        pages.add(new Page(factory.newConstantBytesRefBlockWith(VOCAB[0], 16)));
        pages.add(luceneOneEntry(factory, 32));
        try (BytesRefVector.Builder empty = factory.newBytesRefVectorBuilder(0)) {
            pages.add(new Page(empty.build().asBlock()));
        }
        Page dense = denseOrdinals(factory, 40, 4, false);
        BytesRefBlock col = dense.getBlock(0);
        BytesRefBlock sliced = col.slice(5, 25);
        if (sliced != col) {
            dense.close();
            pages.add(new Page(sliced));
        } else {
            pages.add(dense);
        }
        Page keepSrc = denseOrdinals(factory, 32, 4, false);
        BooleanVector maskVec = null;
        try (BooleanVector.Builder mask = factory.newBooleanVectorBuilder(32)) {
            for (int i = 0; i < 32; i++) {
                mask.appendBoolean(i % 3 != 0);
            }
            maskVec = mask.build();
            BytesRefBlock kept = keepSrc.<BytesRefBlock>getBlock(0).keepMask(maskVec);
            pages.add(new Page(kept));
            keepSrc.close();
            keepSrc = null;
        } catch (RuntimeException e) {
            Releasables.close(keepSrc);
            throw e;
        } finally {
            Releasables.close(maskVec);
        }
        return pages;
    }

    private static Page flat(BlockFactory factory, int n) {
        try (BytesRefVector.Builder b = factory.newBytesRefVectorBuilder(n)) {
            for (int i = 0; i < n; i++) {
                b.appendBytesRef(VOCAB[i % VOCAB.length]);
            }
            return new Page(b.build().asBlock());
        }
    }

    private static Page denseOrdinals(BlockFactory factory, int n, int k, boolean nulls) {
        BytesRefVector dict;
        try (BytesRefVector.Builder db = factory.newBytesRefVectorBuilder(k)) {
            for (int i = 0; i < k; i++) {
                db.appendBytesRef(VOCAB[i % VOCAB.length]);
            }
            dict = db.build();
        }
        if (nulls) {
            try (IntBlock.Builder ob = factory.newIntBlockBuilder(n)) {
                for (int i = 0; i < n; i++) {
                    if (i % 7 == 0) {
                        ob.appendNull();
                    } else {
                        ob.appendInt(i % k);
                    }
                }
                return new Page(new OrdinalBytesRefBlock(ob.build(), dict));
            }
        }
        try (var ob = factory.newIntVectorFixedBuilder(n)) {
            for (int i = 0; i < n; i++) {
                ob.appendInt(i % k);
            }
            return new Page(new OrdinalBytesRefVector(ob.build(), dict).asBlock());
        }
    }

    private static Page paddedOrdinals(BlockFactory factory, int n, int dictSize, int referenced) {
        BytesRefVector dict;
        try (BytesRefVector.Builder db = factory.newBytesRefVectorBuilder(dictSize)) {
            for (int i = 0; i < dictSize; i++) {
                if (i < referenced) {
                    db.appendBytesRef(VOCAB[i % VOCAB.length]);
                } else {
                    db.appendBytesRef(new BytesRef("pad" + i));
                }
            }
            dict = db.build();
        }
        try (var ob = factory.newIntVectorFixedBuilder(n)) {
            for (int i = 0; i < n; i++) {
                ob.appendInt(i % referenced);
            }
            return new Page(new OrdinalBytesRefVector(ob.build(), dict).asBlock());
        }
    }

    private static Page mvOrdinals(BlockFactory factory, int n, int k) {
        BytesRefVector dict;
        try (BytesRefVector.Builder db = factory.newBytesRefVectorBuilder(k)) {
            for (int i = 0; i < k; i++) {
                db.appendBytesRef(VOCAB[i % VOCAB.length]);
            }
            dict = db.build();
        }
        try (IntBlock.Builder ob = factory.newIntBlockBuilder(n)) {
            for (int i = 0; i < n; i++) {
                if (i % 4 == 0) {
                    ob.beginPositionEntry();
                    ob.appendInt(0);
                    ob.appendInt(1);
                    ob.endPositionEntry();
                } else {
                    ob.appendInt(i % k);
                }
            }
            return new Page(new OrdinalBytesRefBlock(ob.build(), dict));
        }
    }

    private static Page allNullOrdinals(BlockFactory factory, int n) {
        BytesRefVector dict;
        try (BytesRefVector.Builder db = factory.newBytesRefVectorBuilder(1)) {
            db.appendBytesRef(VOCAB[0]);
            dict = db.build();
        }
        try (IntBlock.Builder ob = factory.newIntBlockBuilder(n)) {
            for (int i = 0; i < n; i++) {
                ob.appendNull();
            }
            return new Page(new OrdinalBytesRefBlock(ob.build(), dict));
        }
    }

    private static Page luceneOneEntry(BlockFactory factory, int n) {
        BytesRefVector dict;
        try (BytesRefVector.Builder db = factory.newBytesRefVectorBuilder(1)) {
            db.appendBytesRef(VOCAB[0]);
            dict = db.build();
        }
        try (var ob = factory.newIntVectorFixedBuilder(n)) {
            for (int i = 0; i < n; i++) {
                ob.appendInt(0);
            }
            return new Page(new OrdinalBytesRefVector(ob.build(), dict).asBlock());
        }
    }

    private static void assertBlocksEqual(BooleanBlock wrapped, BooleanBlock oracle, int n) {
        assertThat(wrapped.getPositionCount(), equalTo(n));
        assertThat(oracle.getPositionCount(), equalTo(n));
        for (int i = 0; i < n; i++) {
            assertThat("null mismatch at " + i, wrapped.isNull(i), equalTo(oracle.isNull(i)));
            if (wrapped.isNull(i) == false) {
                assertThat(
                    "value mismatch at " + i,
                    wrapped.getBoolean(wrapped.getFirstValueIndex(i)),
                    equalTo(oracle.getBoolean(oracle.getFirstValueIndex(i)))
                );
            }
        }
    }

    private static FieldAttribute field(String name) {
        return new FieldAttribute(
            Source.EMPTY,
            name,
            new EsField(name, DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
    }

    private static Layout layout(FieldAttribute attr) {
        return new Layout.Builder().append(attr).build();
    }
}
