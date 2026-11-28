/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.expression;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.ConstantNullBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.DictionaryPredicateEvaluator.Path;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.TestWarningsSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class DictionaryPredicateEvaluatorTests extends ComputeTestCase {
    private static final BytesRef FOO = new BytesRef("foo");
    private static final BytesRef BAR = new BytesRef("bar");
    private static final BytesRef BAZ = new BytesRef("baz");
    private static final BytesRef QUX = new BytesRef("qux");

    public void testDenseOrdinalsUseDictionaryPath() {
        BlockFactory factory = blockFactory();
        DriverContext ctx = driverContext(factory);
        try (var eval = evaluator(ctx, FOO).get(ctx); Page page = ordinalsPage(factory, 100, List.of(FOO, BAR), 2, false, false)) {
            try (BooleanBlock result = (BooleanBlock) eval.eval(page)) {
                assertThat(((DictionaryPredicateEvaluator) eval).lastPath(), equalTo(Path.DICTIONARY));
                assertPerRowEquals(page, result, FOO);
            }
        }
    }

    public void testReferencedOnlyIgnoresPaddedDictEntries() {
        BlockFactory factory = blockFactory();
        DriverContext ctx = driverContext(factory);
        // Dict has extra "foo" entries that are never referenced. Evaluating them would still be
        // true for == foo, so I4 is: result and (absence of) warnings match per-row.
        List<BytesRef> dict = List.of(FOO, BAR, BAZ, QUX, FOO, FOO);
        try (var eval = evaluator(ctx, FOO).get(ctx); Page page = ordinalsPage(factory, 100, dict, 4, false, false)) {
            try (BooleanBlock result = (BooleanBlock) eval.eval(page)) {
                assertThat(((DictionaryPredicateEvaluator) eval).lastPath(), equalTo(Path.DICTIONARY));
                assertPerRowEquals(page, result, FOO);
            }
            ctx.finish();
            assertThat(ctx.warnings(), equalTo(List.of()));
        }
    }

    public void testConstantVectorBroadcasts() {
        BlockFactory factory = blockFactory();
        DriverContext ctx = driverContext(factory);
        try (var eval = evaluator(ctx, FOO).get(ctx); Page page = new Page(factory.newConstantBytesRefBlockWith(FOO, 100))) {
            try (BooleanBlock result = (BooleanBlock) eval.eval(page)) {
                assertThat(((DictionaryPredicateEvaluator) eval).lastPath(), equalTo(Path.CONSTANT_VECTOR));
                BooleanVector v = result.asVector();
                assertThat(v, not(nullValue()));
                assertTrue(v.isConstant());
                for (int i = 0; i < 100; i++) {
                    assertTrue(v.getBoolean(i));
                }
            }
        }
    }

    public void testAllTrueDictWithoutNullOrdsIsConstant() {
        BlockFactory factory = blockFactory();
        DriverContext ctx = driverContext(factory);
        try (var eval = evaluator(ctx, FOO).get(ctx); Page page = ordinalsPage(factory, 64, List.of(FOO, FOO), 2, false, false)) {
            try (BooleanBlock result = (BooleanBlock) eval.eval(page)) {
                assertThat(((DictionaryPredicateEvaluator) eval).lastPath(), equalTo(Path.DICTIONARY));
                BooleanVector v = result.asVector();
                assertThat(v, not(nullValue()));
                assertTrue("all-true dict with no null ords should be constant", v.isConstant());
            }
        }
    }

    public void testNullOrdsWithNullResultNotConstant() {
        BlockFactory factory = blockFactory();
        DriverContext ctx = driverContext(factory);
        try (var eval = evaluator(ctx, FOO).get(ctx); Page page = ordinalsPage(factory, 64, List.of(FOO, BAR), 2, true, false)) {
            try (BooleanBlock result = (BooleanBlock) eval.eval(page)) {
                assertThat(((DictionaryPredicateEvaluator) eval).lastPath(), equalTo(Path.DICTIONARY));
                assertPerRowEquals(page, result, FOO);
                BooleanVector v = result.asVector();
                assertThat("null ords + null-input null result is not a constant vector", v, nullValue());
            }
        }
    }

    public void testMultivalueFallbackAndWarning() {
        BlockFactory factory = blockFactory();
        DriverContext ctx = driverContext(factory);
        try (var eval = evaluator(ctx, FOO).get(ctx); Page page = ordinalsPage(factory, 32, List.of(FOO, BAR), 2, false, true)) {
            try (BooleanBlock result = (BooleanBlock) eval.eval(page)) {
                assertThat(((DictionaryPredicateEvaluator) eval).lastPath(), equalTo(Path.PER_ROW));
                for (int i = 0; i < result.getPositionCount(); i++) {
                    BytesRefBlock input = page.getBlock(0);
                    if (input.getValueCount(i) > 1) {
                        assertTrue(result.isNull(i));
                    }
                }
            }
            ctx.finish();
            assertFalse(ctx.warnings().isEmpty());
            assertThat(ctx.warnings().toString(), containsString("multi-value"));
        }
    }

    public void testPlainAndConstantNullFallback() {
        BlockFactory factory = blockFactory();
        DriverContext ctx = driverContext(factory);
        try (var eval = evaluator(ctx, FOO).get(ctx)) {
            try (Page plain = flatPage(factory, 32, List.of(FOO, BAR)); BooleanBlock result = (BooleanBlock) eval.eval(plain)) {
                assertThat(((DictionaryPredicateEvaluator) eval).lastPath(), equalTo(Path.PER_ROW));
                assertPerRowEquals(plain, result, FOO);
            }
            try (Page nulls = new Page(factory.newConstantNullBlock(16)); BooleanBlock result = (BooleanBlock) eval.eval(nulls)) {
                assertThat(((DictionaryPredicateEvaluator) eval).lastPath(), equalTo(Path.PER_ROW));
                assertThat(result.getPositionCount(), equalTo(16));
                for (int i = 0; i < 16; i++) {
                    assertTrue("row " + i, result.isNull(i));
                }
            }
        }
    }

    public void testBelowMinPositionsFallsBack() {
        BlockFactory factory = blockFactory();
        DriverContext ctx = driverContext(factory);
        try (var eval = evaluator(ctx, FOO).get(ctx); Page page = ordinalsPage(factory, 8, List.of(FOO, BAR), 2, false, false)) {
            try (BooleanBlock result = (BooleanBlock) eval.eval(page)) {
                assertThat(((DictionaryPredicateEvaluator) eval).lastPath(), equalTo(Path.PER_ROW));
                assertPerRowEquals(page, result, FOO);
            }
        }
    }

    public void testDensityGateSkip() {
        BlockFactory factory = blockFactory();
        DriverContext ctx = driverContext(factory);
        // n=20, 20 unique ords: 2d > n, skip dictionary path
        List<BytesRef> dict = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            dict.add(new BytesRef("v" + i));
        }
        try (var eval = evaluator(ctx, FOO).get(ctx); Page page = ordinalsPage(factory, 20, dict, 20, false, false)) {
            try (BooleanBlock result = (BooleanBlock) eval.eval(page)) {
                assertThat(((DictionaryPredicateEvaluator) eval).lastPath(), equalTo(Path.PER_ROW));
                assertPerRowEquals(page, result, FOO);
            }
        }
    }

    public void testDictRefcountUnchanged() {
        BlockFactory factory = blockFactory();
        DriverContext ctx = driverContext(factory);
        BytesRefVector dict;
        try (var eval = evaluator(ctx, FOO).get(ctx)) {
            try (Page page = ordinalsPage(factory, 64, List.of(FOO, BAR, BAZ, QUX), 4, false, false)) {
                dict = page.<BytesRefBlock>getBlock(0).asOrdinals().getDictionaryVector();
                try (BooleanBlock result = (BooleanBlock) eval.eval(page)) {
                    assertThat(((DictionaryPredicateEvaluator) eval).lastPath(), equalTo(Path.DICTIONARY));
                    assertTrue(dict.hasReferences());
                }
            }
            assertFalse("input dictionary must not be retained after the page is closed", dict.hasReferences());
        }
    }

    public void testInnerIaeFallsBackAndKeepsDictAlive() {
        BlockFactory factory = blockFactory();
        DriverContext ctx = driverContext(factory);
        AtomicInteger dictEvals = new AtomicInteger();
        ExpressionEvaluator.Factory throwing = c -> new ExpressionEvaluator() {
            @Override
            public Block eval(Page page) {
                dictEvals.incrementAndGet();
                throw new IllegalArgumentException("boom");
            }

            @Override
            public long baseRamBytesUsed() {
                return 1;
            }

            @Override
            public void close() {}
        };
        try (
            var eval = new DictionaryPredicateEvaluator.Factory(equalsFactory(FOO), throwing, 0).get(ctx);
            Page page = ordinalsPage(factory, 64, List.of(FOO, BAR), 2, false, false)
        ) {
            BytesRefVector dict = page.<BytesRefBlock>getBlock(0).asOrdinals().getDictionaryVector();
            try (BooleanBlock result = (BooleanBlock) eval.eval(page)) {
                assertThat(((DictionaryPredicateEvaluator) eval).lastPath(), equalTo(Path.PER_ROW));
                assertThat(dictEvals.get(), greaterThan(0));
                assertTrue(dict.hasReferences());
                assertPerRowEquals(page, result, FOO);
            }
        }
    }

    public void testCircuitBreakingExceptionPropagates() {
        BlockFactory factory = blockFactory();
        DriverContext ctx = driverContext(factory);
        CircuitBreakingException cbe = new CircuitBreakingException("cbe", CircuitBreaker.Durability.PERMANENT);
        ExpressionEvaluator.Factory throwing = c -> new ExpressionEvaluator() {
            @Override
            public Block eval(Page page) {
                throw cbe;
            }

            @Override
            public long baseRamBytesUsed() {
                return 1;
            }

            @Override
            public void close() {}
        };
        try (
            var eval = new DictionaryPredicateEvaluator.Factory(equalsFactory(FOO), throwing, 0).get(ctx);
            Page page = ordinalsPage(factory, 64, List.of(FOO, BAR), 2, false, false)
        ) {
            CircuitBreakingException thrown = expectThrows(CircuitBreakingException.class, () -> eval.eval(page));
            assertThat(thrown, sameInstance(cbe));
        }
    }

    public void testCloseReleasesBothTrees() {
        AtomicBoolean perRowClosed = new AtomicBoolean();
        AtomicBoolean dictClosed = new AtomicBoolean();
        BlockFactory factory = blockFactory();
        DriverContext ctx = driverContext(factory);
        var eval = new DictionaryPredicateEvaluator.Factory(
            tracking(equalsFactory(FOO), perRowClosed),
            tracking(equalsFactory(FOO), dictClosed),
            0
        ).get(ctx);
        eval.close();
        assertTrue(perRowClosed.get());
        assertTrue(dictClosed.get());
    }

    public void testToStringDelegatesToPerRow() {
        BlockFactory factory = blockFactory();
        DriverContext ctx = driverContext(factory);
        var factoryEval = evaluator(ctx, FOO);
        try (var eval = factoryEval.get(ctx)) {
            assertThat(factoryEval.toString(), equalTo("EqualsLiteral[foo]"));
            assertThat(eval.toString(), equalTo("EqualsLiteral[foo]"));
        }
    }

    public void testProductionShapedUsesDictionaryHighCardSkips() {
        BlockFactory factory = blockFactory();
        DriverContext ctx = driverContext(factory);
        try (var eval = evaluator(ctx, FOO).get(ctx)) {
            try (Page tiny = ordinalsPage(factory, 1000, List.of(FOO, BAR, BAZ, QUX), 4, false, false)) {
                try (Block result = eval.eval(tiny)) {
                    assertThat(((DictionaryPredicateEvaluator) eval).lastPath(), equalTo(Path.DICTIONARY));
                    assertThat(result.getPositionCount(), equalTo(1000));
                }
            }
            List<BytesRef> unique = new ArrayList<>();
            for (int i = 0; i < 1000; i++) {
                unique.add(new BytesRef("u" + i));
            }
            try (Page high = ordinalsPage(factory, 1000, unique, 1000, false, false)) {
                try (Block result = eval.eval(high)) {
                    assertThat(((DictionaryPredicateEvaluator) eval).lastPath(), equalTo(Path.PER_ROW));
                    assertThat(result.getPositionCount(), equalTo(1000));
                }
            }
        }
    }

    public void testCrankyBreaker() {
        testWithCrankyBlockFactory(factory -> {
            DriverContext ctx = new DriverContext(factory.bigArrays(), factory, null);
            try (var eval = evaluator(ctx, FOO).get(ctx); Page page = ordinalsPage(factory, 64, List.of(FOO, BAR), 2, false, false)) {
                try (Block result = eval.eval(page)) {
                    assertThat(result.getPositionCount(), equalTo(64));
                }
            }
        });
    }

    private static DriverContext driverContext(BlockFactory factory) {
        return new DriverContext(factory.bigArrays(), factory, null);
    }

    private static ExpressionEvaluator.Factory evaluator(DriverContext unused, BytesRef literal) {
        return new DictionaryPredicateEvaluator.Factory(equalsFactory(literal), equalsFactory(literal), 0);
    }

    private static ExpressionEvaluator.Factory equalsFactory(BytesRef literal) {
        return new ExpressionEvaluator.Factory() {
            @Override
            public ExpressionEvaluator get(DriverContext context) {
                return new EqualsLiteral(context, literal);
            }

            @Override
            public String toString() {
                return "EqualsLiteral[" + literal.utf8ToString() + "]";
            }
        };
    }

    private static ExpressionEvaluator.Factory tracking(ExpressionEvaluator.Factory inner, AtomicBoolean closed) {
        return context -> {
            ExpressionEvaluator e = inner.get(context);
            return new ExpressionEvaluator() {
                @Override
                public Block eval(Page page) {
                    return e.eval(page);
                }

                @Override
                public long baseRamBytesUsed() {
                    return e.baseRamBytesUsed();
                }

                @Override
                public void close() {
                    closed.set(true);
                    e.close();
                }

                @Override
                public String toString() {
                    return e.toString();
                }
            };
        };
    }

    private static Page ordinalsPage(
        BlockFactory factory,
        int n,
        List<BytesRef> dictValues,
        int referenced,
        boolean nulls,
        boolean multivalue
    ) {
        BytesRefVector dict;
        try (BytesRefVector.Builder db = factory.newBytesRefVectorBuilder(dictValues.size())) {
            for (BytesRef v : dictValues) {
                db.appendBytesRef(v);
            }
            dict = db.build();
        }
        if (multivalue) {
            try (IntBlock.Builder ob = factory.newIntBlockBuilder(n)) {
                for (int i = 0; i < n; i++) {
                    if (i % 5 == 0) {
                        ob.beginPositionEntry();
                        ob.appendInt(0);
                        ob.appendInt(1);
                        ob.endPositionEntry();
                    } else {
                        ob.appendInt(i % referenced);
                    }
                }
                return new Page(new OrdinalBytesRefBlock(ob.build(), dict));
            }
        }
        if (nulls) {
            try (IntBlock.Builder ob = factory.newIntBlockBuilder(n)) {
                for (int i = 0; i < n; i++) {
                    if (i % 7 == 0) {
                        ob.appendNull();
                    } else {
                        ob.appendInt(i % referenced);
                    }
                }
                return new Page(new OrdinalBytesRefBlock(ob.build(), dict));
            }
        }
        try (var ob = factory.newIntVectorFixedBuilder(n)) {
            for (int i = 0; i < n; i++) {
                ob.appendInt(i % referenced);
            }
            return new Page(new OrdinalBytesRefVector(ob.build(), dict).asBlock());
        }
    }

    private static Page flatPage(BlockFactory factory, int n, List<BytesRef> values) {
        try (BytesRefVector.Builder b = factory.newBytesRefVectorBuilder(n)) {
            for (int i = 0; i < n; i++) {
                b.appendBytesRef(values.get(i % values.size()));
            }
            return new Page(b.build().asBlock());
        }
    }

    private static void assertPerRowEquals(Page page, BooleanBlock result, BytesRef literal) {
        BytesRefBlock input = page.getBlock(0);
        BytesRef scratch = new BytesRef();
        assertThat(result.getPositionCount(), equalTo(input.getPositionCount()));
        for (int i = 0; i < input.getPositionCount(); i++) {
            if (input.isNull(i) || input.getValueCount(i) != 1) {
                assertTrue("row " + i, result.isNull(i));
                continue;
            }
            boolean expected = input.getBytesRef(input.getFirstValueIndex(i), scratch).equals(literal);
            assertFalse("row " + i, result.isNull(i));
            assertThat("row " + i, result.getBoolean(result.getFirstValueIndex(i)), equalTo(expected));
        }
    }

    private static final class EqualsLiteral implements ExpressionEvaluator {
        private final DriverContext ctx;
        private final BytesRef literal;
        private Warnings warnings;

        EqualsLiteral(DriverContext ctx, BytesRef literal) {
            this.ctx = ctx;
            this.literal = literal;
        }

        @Override
        public Block eval(Page page) {
            BytesRefBlock in = page.getBlock(0);
            if (in instanceof ConstantNullBlock || in.areAllValuesNull()) {
                return ctx.blockFactory().newConstantNullBlock(in.getPositionCount());
            }
            BytesRef scratch = new BytesRef();
            try (BooleanBlock.Builder out = ctx.blockFactory().newBooleanBlockBuilder(in.getPositionCount())) {
                for (int p = 0; p < in.getPositionCount(); p++) {
                    if (in.isNull(p)) {
                        out.appendNull();
                        continue;
                    }
                    if (in.getValueCount(p) != 1) {
                        warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
                        out.appendNull();
                        continue;
                    }
                    out.appendBoolean(in.getBytesRef(in.getFirstValueIndex(p), scratch).equals(literal));
                }
                return out.build();
            }
        }

        private Warnings warnings() {
            if (warnings == null) {
                warnings = ctx.createWarnings(new TestWarningsSource("a == \"foo\"", null, 1, 1));
            }
            return warnings;
        }

        @Override
        public long baseRamBytesUsed() {
            return 1;
        }

        @Override
        public void close() {}

        @Override
        public String toString() {
            return "EqualsLiteral[" + literal.utf8ToString() + "]";
        }
    }
}
