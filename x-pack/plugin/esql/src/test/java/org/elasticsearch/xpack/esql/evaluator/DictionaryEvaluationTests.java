/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.evaluator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.DictionaryPredicateEvaluator;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.lucene.EmptyIndexedByShardId;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.TestBlockFactory;
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
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Concat;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.EndsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.StartsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.ToLower;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.ToUpper;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.RLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.WildcardLike;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.BooleanLogicExpressionEvaluator;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.InsensitiveEquals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.planner.Layout;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class DictionaryEvaluationTests extends ESTestCase {
    private final FieldAttribute a = keyword("a");
    private final FieldAttribute b = keyword("b");
    private final Layout layoutA = layout(a);
    private final EvaluatorMapper.ToEvaluator parent = EvalMapper.toEvaluatorContext(
        FoldContext.small(),
        layoutA,
        EmptyIndexedByShardId.instance(),
        null
    );

    public void testWrapEqualsAndNotEquals() {
        assertThat(
            wrap(new Equals(Source.EMPTY, a, Literal.keyword(Source.EMPTY, "x"))),
            instanceOf(DictionaryPredicateEvaluator.Factory.class)
        );
        assertThat(
            wrap(new NotEquals(Source.EMPTY, Literal.keyword(Source.EMPTY, "x"), a)),
            instanceOf(DictionaryPredicateEvaluator.Factory.class)
        );
    }

    public void testNoWrapTwoFieldsIpLongFoldable() {
        Layout two = layout(a, b);
        EvaluatorMapper.ToEvaluator twoParent = EvalMapper.toEvaluatorContext(
            FoldContext.small(),
            two,
            EmptyIndexedByShardId.instance(),
            null
        );
        assertThat(DictionaryEvaluation.maybeWrap(new Equals(Source.EMPTY, a, b), two, twoParent), nullValue());

        FieldAttribute ip = field("ip", DataType.IP);
        assertThat(
            DictionaryEvaluation.maybeWrap(new Equals(Source.EMPTY, ip, Literal.keyword(Source.EMPTY, "1.2.3.4")), layout(ip), parent),
            nullValue()
        );

        FieldAttribute lng = field("n", DataType.LONG);
        assertThat(
            DictionaryEvaluation.maybeWrap(
                new Equals(Source.EMPTY, lng, new Literal(Source.EMPTY, 1L, DataType.LONG)),
                layout(lng),
                parent
            ),
            nullValue()
        );

        Equals foldable = new Equals(Source.EMPTY, Literal.keyword(Source.EMPTY, "x"), Literal.keyword(Source.EMPTY, "x"));
        assertTrue(foldable.foldable());
        assertThat(wrap(foldable), nullValue());
    }

    public void testToStringEqualsUnwrapped() {
        Equals exp = new Equals(Source.EMPTY, a, Literal.keyword(Source.EMPTY, "x"));
        ExpressionEvaluator.Factory wrapped = EvalMapper.toEvaluator(FoldContext.small(), exp, layoutA);
        ExpressionEvaluator.Factory unwrapped = ((EvaluatorMapper) exp).toEvaluator(EvalMapper.toEvaluatorContext(layoutA, parent));
        assertThat(wrapped, instanceOf(DictionaryPredicateEvaluator.Factory.class));
        assertThat(wrapped.toString(), equalTo(unwrapped.toString()));
        DriverContext ctx = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, TestBlockFactory.getNonBreakingInstance(), null);
        try (ExpressionEvaluator we = wrapped.get(ctx); ExpressionEvaluator ue = unwrapped.get(ctx)) {
            assertThat(we.toString(), equalTo(ue.toString()));
        }
    }

    public void testToLowerEqualsIsWrapped() {
        Equals exp = new Equals(Source.EMPTY, new ToLower(Source.EMPTY, a, EsqlTestUtils.TEST_CFG), Literal.keyword(Source.EMPTY, "x"));
        assertThat(wrap(exp), instanceOf(DictionaryPredicateEvaluator.Factory.class));
    }

    public void testWholeAndWrapVsTwoLeafWrap() {
        And sameField = new And(
            Source.EMPTY,
            new NotEquals(Source.EMPTY, a, Literal.keyword(Source.EMPTY, "-")),
            new NotEquals(Source.EMPTY, a, Literal.keyword(Source.EMPTY, "y"))
        );
        assertThat(wrap(sameField), instanceOf(DictionaryPredicateEvaluator.Factory.class));

        Layout two = layout(a, b);
        EvaluatorMapper.ToEvaluator twoParent = EvalMapper.toEvaluatorContext(
            FoldContext.small(),
            two,
            EmptyIndexedByShardId.instance(),
            null
        );
        And twoFields = new And(
            Source.EMPTY,
            new NotEquals(Source.EMPTY, a, Literal.keyword(Source.EMPTY, "-")),
            new Equals(Source.EMPTY, b, Literal.keyword(Source.EMPTY, "x"))
        );
        ExpressionEvaluator.Factory andFactory = DictionaryEvaluation.maybeWrap(twoFields, two, twoParent);
        assertThat(andFactory, nullValue());
        ExpressionEvaluator.Factory planned = EvalMapper.toEvaluator(FoldContext.small(), twoFields, two);
        assertThat(planned, instanceOf(BooleanLogicExpressionEvaluator.Factory.class));
        BooleanLogicExpressionEvaluator.Factory andFac = (BooleanLogicExpressionEvaluator.Factory) planned;
        assertThat(andFac.leftEval(), instanceOf(DictionaryPredicateEvaluator.Factory.class));
        assertThat(andFac.rightEval(), instanceOf(DictionaryPredicateEvaluator.Factory.class));
        assertThat(planned.toString(), containsString("Equals"));
    }

    public void testIsNullOrEqualsNullRowIsTrue() {
        Or exp = new Or(Source.EMPTY, new IsNull(Source.EMPTY, a), new Equals(Source.EMPTY, a, Literal.keyword(Source.EMPTY, "x")));
        assertThat(wrap(exp), instanceOf(DictionaryPredicateEvaluator.Factory.class));
        DriverContext ctx = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, TestBlockFactory.getNonBreakingInstance(), null);
        var factory = EvalMapper.toEvaluator(FoldContext.small(), exp, layoutA);
        try (
            ExpressionEvaluator eval = factory.get(ctx);
            Page page = new Page(ctx.blockFactory().newConstantNullBlock(4));
            BooleanBlock result = (BooleanBlock) eval.eval(page)
        ) {
            assertThat(((DictionaryPredicateEvaluator) eval).lastPath(), equalTo(DictionaryPredicateEvaluator.Path.PER_ROW));
            for (int i = 0; i < 4; i++) {
                assertFalse(result.isNull(i));
                assertTrue("null row of (a IS NULL OR a==x) is true", result.getBoolean(result.getFirstValueIndex(i)));
            }
        }
    }

    public void testIsNullOrEqualsNullOrdinalsUseDictionaryPath() {
        Or exp = new Or(Source.EMPTY, new IsNull(Source.EMPTY, a), new Equals(Source.EMPTY, a, Literal.keyword(Source.EMPTY, "x")));
        DriverContext ctx = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, TestBlockFactory.getNonBreakingInstance(), null);
        var factory = EvalMapper.toEvaluator(FoldContext.small(), exp, layoutA);
        BytesRef x = new BytesRef("x");
        BytesRef y = new BytesRef("y");
        try (
            ExpressionEvaluator eval = factory.get(ctx);
            Page page = ordinalsWithNulls(ctx.blockFactory(), 64, List.of(x, y));
            BooleanBlock result = (BooleanBlock) eval.eval(page)
        ) {
            assertThat(eval, instanceOf(DictionaryPredicateEvaluator.class));
            assertThat(((DictionaryPredicateEvaluator) eval).lastPath(), equalTo(DictionaryPredicateEvaluator.Path.DICTIONARY));
            BytesRefBlock input = page.getBlock(0);
            for (int i = 0; i < 64; i++) {
                assertFalse("row " + i, result.isNull(i));
                boolean expected = input.isNull(i)
                    || input.getBytesRef(input.getFirstValueIndex(i), new BytesRef()).utf8ToString().equals("x");
                assertThat("row " + i, result.getBoolean(result.getFirstValueIndex(i)), equalTo(expected));
            }
        }
    }

    public void testRealEqualsOnOrdinalsUsesDictionaryPath() {
        Equals exp = new Equals(Source.EMPTY, a, Literal.keyword(Source.EMPTY, "foo"));
        DriverContext ctx = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, TestBlockFactory.getNonBreakingInstance(), null);
        var factory = EvalMapper.toEvaluator(FoldContext.small(), exp, layoutA);
        BytesRef foo = new BytesRef("foo");
        BytesRef bar = new BytesRef("bar");
        try (
            ExpressionEvaluator eval = factory.get(ctx);
            Page page = ordinalsWithNulls(ctx.blockFactory(), 100, List.of(foo, bar));
            BooleanBlock result = (BooleanBlock) eval.eval(page)
        ) {
            assertThat(eval, instanceOf(DictionaryPredicateEvaluator.class));
            assertThat(((DictionaryPredicateEvaluator) eval).lastPath(), equalTo(DictionaryPredicateEvaluator.Path.DICTIONARY));
            BytesRefBlock input = page.getBlock(0);
            BytesRef scratch = new BytesRef();
            for (int i = 0; i < 100; i++) {
                if (input.isNull(i)) {
                    assertTrue("row " + i, result.isNull(i));
                } else {
                    assertFalse("row " + i, result.isNull(i));
                    assertThat(
                        "row " + i,
                        result.getBoolean(result.getFirstValueIndex(i)),
                        equalTo(input.getBytesRef(input.getFirstValueIndex(i), scratch).equals(foo))
                    );
                }
            }
        }
        ctx.finish();
        assertThat(ctx.warnings(), equalTo(List.of()));
    }

    public void testNotLikeIsWrappedBareIsNullIsNot() {
        Not notLike = new Not(Source.EMPTY, new WildcardLike(Source.EMPTY, a, new WildcardPattern("f*")));
        assertThat(wrap(notLike), instanceOf(DictionaryPredicateEvaluator.Factory.class));
        assertThat(wrap(new IsNull(Source.EMPTY, a)), nullValue());
        assertThat(wrap(new IsNotNull(Source.EMPTY, a)), nullValue());
    }

    public void testInStartsWithInsensitive() {
        assertThat(
            wrap(new In(Source.EMPTY, a, List.of(Literal.keyword(Source.EMPTY, "x"), Literal.keyword(Source.EMPTY, "y")))),
            instanceOf(DictionaryPredicateEvaluator.Factory.class)
        );
        assertThat(
            wrap(new StartsWith(Source.EMPTY, a, Literal.keyword(Source.EMPTY, "fo"))),
            instanceOf(DictionaryPredicateEvaluator.Factory.class)
        );
        assertThat(
            wrap(new InsensitiveEquals(Source.EMPTY, a, Literal.keyword(Source.EMPTY, "X"))),
            instanceOf(DictionaryPredicateEvaluator.Factory.class)
        );
        assertThat(wrap(new RLike(Source.EMPTY, a, new RLikePattern("f.*"))), instanceOf(DictionaryPredicateEvaluator.Factory.class));
    }

    public void testConcatNotAllowlisted() {
        Equals exp = new Equals(
            Source.EMPTY,
            new Concat(Source.EMPTY, a, List.of(Literal.keyword(Source.EMPTY, "!"))),
            Literal.keyword(Source.EMPTY, "x!")
        );
        assertThat(wrap(exp), nullValue());
    }

    public void testAllowlistedEvaluatorsHaveEmptyWarnExceptions() throws IOException {
        Path root = esqlSourceRoot();
        List<String> files = List.of(
            "expression/predicate/operator/comparison/Equals.java",
            "expression/predicate/operator/comparison/NotEquals.java",
            "expression/predicate/operator/comparison/In.java",
            "expression/predicate/operator/comparison/InsensitiveEquals.java",
            "expression/function/scalar/string/StartsWith.java",
            "expression/function/scalar/string/EndsWith.java",
            "expression/function/scalar/string/ChangeCase.java",
            "expression/function/scalar/string/AutomataMatch.java",
            "expression/function/scalar/string/regex/WildcardLike.java",
            "expression/function/scalar/string/regex/RLike.java",
            "expression/function/scalar/string/regex/WildcardLikeList.java",
            "expression/function/scalar/string/regex/RLikeList.java"
        );
        int annotations = 0;
        for (String rel : files) {
            Path source = root.resolve(rel);
            assertTrue("missing " + source.toAbsolutePath(), Files.exists(source));
            String text = Files.readString(source);
            annotations += assertNoWarnExceptions(source, text);
        }
        assertThat("scanned evaluator annotations", annotations, greaterThan(0));
    }

    public void testAllowlistedShapesDoNotWarnOnOddSingleValues() {
        List<BytesRef> dict = List.of(
            new BytesRef(""),
            new BytesRef("foo"),
            new BytesRef("x".repeat(4096)),
            new BytesRef("日本語")
        );
        List<Expression> shapes = List.of(
            new Equals(Source.EMPTY, a, Literal.keyword(Source.EMPTY, "foo")),
            new NotEquals(Source.EMPTY, a, Literal.keyword(Source.EMPTY, "-")),
            new In(Source.EMPTY, a, List.of(Literal.keyword(Source.EMPTY, "foo"), Literal.keyword(Source.EMPTY, "bar"))),
            new InsensitiveEquals(Source.EMPTY, a, Literal.keyword(Source.EMPTY, "FOO")),
            new StartsWith(Source.EMPTY, a, Literal.keyword(Source.EMPTY, "f")),
            new EndsWith(Source.EMPTY, a, Literal.keyword(Source.EMPTY, "o")),
            new WildcardLike(Source.EMPTY, a, new WildcardPattern("f*")),
            new RLike(Source.EMPTY, a, new RLikePattern("f.*")),
            new Equals(Source.EMPTY, new ToLower(Source.EMPTY, a, EsqlTestUtils.TEST_CFG), Literal.keyword(Source.EMPTY, "foo")),
            new Equals(Source.EMPTY, new ToUpper(Source.EMPTY, a, EsqlTestUtils.TEST_CFG), Literal.keyword(Source.EMPTY, "FOO")),
            new Not(Source.EMPTY, new WildcardLike(Source.EMPTY, a, new WildcardPattern("f*"))),
            new Or(Source.EMPTY, new IsNull(Source.EMPTY, a), new Equals(Source.EMPTY, a, Literal.keyword(Source.EMPTY, "foo")))
        );
        BlockFactory factory = TestBlockFactory.getNonBreakingInstance();
        try (Page page = ordinalsNoNulls(factory, 64, dict)) {
            for (Expression exp : shapes) {
                DriverContext ctx = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, factory, null);
                var fac = EvalMapper.toEvaluator(FoldContext.small(), exp, layoutA);
                try (ExpressionEvaluator eval = fac.get(ctx); BooleanBlock result = (BooleanBlock) eval.eval(page)) {
                    assertThat(exp + " should wrap", eval, instanceOf(DictionaryPredicateEvaluator.class));
                    assertThat(
                        exp.toString(),
                        ((DictionaryPredicateEvaluator) eval).lastPath(),
                        equalTo(DictionaryPredicateEvaluator.Path.DICTIONARY)
                    );
                    assertThat(result.getPositionCount(), equalTo(64));
                }
                ctx.finish();
                assertThat(exp + " warnings " + ctx.warnings(), ctx.warnings(), equalTo(List.of()));
            }
        }
    }

    private ExpressionEvaluator.Factory wrap(Expression exp) {
        return DictionaryEvaluation.maybeWrap(exp, layoutA, parent);
    }

    private static Path esqlSourceRoot() {
        Path dir = Path.of("").toAbsolutePath();
        while (dir != null) {
            Path candidate = dir.resolve("src/main/java/org/elasticsearch/xpack/esql");
            if (Files.isDirectory(candidate)) {
                return candidate;
            }
            Path fromRepo = dir.resolve("x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql");
            if (Files.isDirectory(fromRepo)) {
                return fromRepo;
            }
            dir = dir.getParent();
        }
        throw new AssertionError("esql sources not found walking from " + Path.of("").toAbsolutePath());
    }

    private static int assertNoWarnExceptions(Path source, String text) {
        int count = 0;
        int i = 0;
        while (i < text.length()) {
            int eval = text.indexOf("@Evaluator", i);
            int convert = text.indexOf("@ConvertEvaluator", i);
            int at;
            String name;
            if (eval < 0 && convert < 0) {
                break;
            }
            if (eval >= 0 && (convert < 0 || eval < convert)) {
                at = eval;
                name = "@Evaluator";
            } else {
                at = convert;
                name = "@ConvertEvaluator";
            }
            int p = at + name.length();
            while (p < text.length() && Character.isWhitespace(text.charAt(p))) {
                p++;
            }
            int end = p;
            if (p < text.length() && text.charAt(p) == '(') {
                int depth = 1;
                end = p + 1;
                while (end < text.length() && depth > 0) {
                    char c = text.charAt(end);
                    if (c == '(') {
                        depth++;
                    } else if (c == ')') {
                        depth--;
                    }
                    end++;
                }
            }
            String span = text.substring(at, end);
            assertFalse(source + " has warnExceptions: " + span, span.contains("warnExceptions"));
            count++;
            i = Math.max(end, at + name.length());
        }
        return count;
    }

    private static Page ordinalsNoNulls(BlockFactory factory, int n, List<BytesRef> dictValues) {
        BytesRefVector dict;
        try (BytesRefVector.Builder db = factory.newBytesRefVectorBuilder(dictValues.size())) {
            for (BytesRef v : dictValues) {
                db.appendBytesRef(v);
            }
            dict = db.build();
        }
        try (var ob = factory.newIntVectorFixedBuilder(n)) {
            for (int i = 0; i < n; i++) {
                ob.appendInt(i % dictValues.size());
            }
            return new Page(new OrdinalBytesRefVector(ob.build(), dict).asBlock());
        }
    }

    private static Page ordinalsWithNulls(BlockFactory factory, int n, List<BytesRef> dictValues) {
        BytesRefVector dict;
        try (BytesRefVector.Builder db = factory.newBytesRefVectorBuilder(dictValues.size())) {
            for (BytesRef v : dictValues) {
                db.appendBytesRef(v);
            }
            dict = db.build();
        }
        try (IntBlock.Builder ob = factory.newIntBlockBuilder(n)) {
            for (int i = 0; i < n; i++) {
                if (i % 7 == 0) {
                    ob.appendNull();
                } else {
                    ob.appendInt(i % dictValues.size());
                }
            }
            return new Page(new OrdinalBytesRefBlock(ob.build(), dict));
        }
    }

    private static FieldAttribute keyword(String name) {
        return field(name, DataType.KEYWORD);
    }

    private static FieldAttribute field(String name, DataType type) {
        return new FieldAttribute(Source.EMPTY, name, new EsField(name, type, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
    }

    private static Layout layout(FieldAttribute... attrs) {
        Layout.Builder b = new Layout.Builder();
        b.append(List.of(attrs));
        return b.build();
    }
}
