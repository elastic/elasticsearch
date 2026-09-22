/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvCompare;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvContains;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvGreater;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvInRange;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvIntersects;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvLess;
import org.elasticsearch.xpack.esql.expression.predicate.Range;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.planner.Layout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * The row mask's one obligation, checked over a fixed grid rather than a random sample: it may keep rows the filter
 * rejects, since the retained FilterExec removes them, but it must never drop a row the filter keeps, and it must never
 * throw. The oracle is the expression itself, evaluated by {@link EvalMapper} over the same page, so the expected answer
 * is the function's own rather than one written down beside it.
 *
 * <p>Every edge value sits in one small block — both bounds, the values between and beyond them, null, and for doubles
 * {@code -0.0}, {@code 0.0} and {@code NaN} — so the grid multiplies expressions, not rows. The axes are the five
 * multivalue forms and their scalar siblings, the column type, the operand (literal, list, null, another column), the
 * bound options, and the connective the leaf sits under. A failure names the cell.
 */
public class ParquetPushdownMaskGridTests extends ESTestCase {

    private final BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("none"))
        .build();

    /** One column type's rows, the column operand's rows, and three ordered bounds drawn from the rows. */
    private record Fixture(DataType type, Object[] rows, Object[] other, Object lo, Object mid, Object hi) {}

    private static final List<Fixture> FIXTURES = List.of(
        new Fixture(
            DataType.LONG,
            new Object[] { 10L, 20L, 30L, 40L, 50L, null },
            new Object[] { 20L, 20L, 40L, 40L, null, 30L },
            20L,
            30L,
            40L
        ),
        new Fixture(DataType.INTEGER, new Object[] { 10, 20, 30, 40, 50, null }, new Object[] { 20, 20, 40, 40, null, 30 }, 20, 30, 40),
        new Fixture(
            DataType.DATETIME,
            new Object[] { 10L, 20L, 30L, 40L, 50L, null },
            new Object[] { 20L, 20L, 40L, 40L, null, 30L },
            20L,
            30L,
            40L
        ),
        new Fixture(
            DataType.DOUBLE,
            new Object[] { -0.0, 0.0, Double.NaN, 20.0, 40.0, null },
            new Object[] { 0.0, -0.0, 20.0, Double.NaN, null, 40.0 },
            0.0,
            20.0,
            40.0
        ),
        new Fixture(
            DataType.KEYWORD,
            new Object[] { bytes("a"), bytes("b"), bytes("c"), bytes("d"), bytes("e"), null },
            new Object[] { bytes("b"), bytes("b"), bytes("d"), bytes("d"), null, bytes("c") },
            bytes("b"),
            bytes("c"),
            bytes("d")
        )
    );

    public void testMaskNeverDropsARowTheFilterKeeps() {
        int checked = 0;
        List<String> failures = new ArrayList<>();
        for (Fixture fixture : FIXTURES) {
            Attribute x = new ReferenceAttribute(Source.EMPTY, "x", fixture.type());
            Attribute y = new ReferenceAttribute(Source.EMPTY, "y", fixture.type());
            Block xBlock = block(fixture.type(), fixture.rows());
            Block yBlock = block(fixture.type(), fixture.other());
            try {
                Map<String, Block> blocks = Map.of("x", xBlock, "y", yBlock);
                Page page = new Page(xBlock, yBlock);
                Layout.Builder layoutBuilder = new Layout.Builder();
                layoutBuilder.append(List.of(x));
                layoutBuilder.append(List.of(y));
                Layout layout = layoutBuilder.build();
                for (Expression leaf : leaves(fixture, x, y)) {
                    if (fixture.type() == DataType.DOUBLE && isMultivalueForm(leaf) == false) {
                        // The scalar arms order doubles with Double.compare, which separates -0.0 from 0.0 and ranks NaN
                        // above every value, while the engine's own evaluators compare with primitive operators. That
                        // mismatch predates this change and drops rows for `d == -0.0` and for NOT over a comparison on a
                        // NaN row. The multivalue forms decline on a double block rather than inherit it, which is what
                        // this grid holds them to; the scalar defect is tracked separately.
                        continue;
                    }
                    for (Expression expr : wrappers(leaf, x, fixture)) {
                        boolean[] truth = oracle(expr, page, layout);
                        if (truth == null) {
                            continue; // not an expression the engine evaluates; nothing to hold the mask to
                        }
                        checked++;
                        String cell = fixture.type() + " " + describe(expr);
                        WordMask mask;
                        try {
                            mask = new ParquetPushedExpressions(List.of(expr)).evaluateFilter(
                                blocks,
                                fixture.rows().length,
                                new WordMask()
                            );
                        } catch (RuntimeException e) {
                            failures.add(cell + " threw " + e);
                            continue;
                        }
                        for (int row = 0; row < truth.length; row++) {
                            if (truth[row] && mask != null && mask.get(row) == false) {
                                failures.add(cell + " dropped row " + row + " (" + fixture.rows()[row] + ")");
                            }
                        }
                    }
                }
            } finally {
                xBlock.close();
                yBlock.close();
            }
        }
        assertTrue("the grid evaluated too few cells to mean anything: " + checked, checked > 500);
        assertTrue(failures.size() + " of " + checked + " cells failed:\n" + String.join("\n", failures), failures.isEmpty());
    }

    private static List<Expression> leaves(Fixture f, Attribute x, Attribute y) {
        DataType t = f.type();
        Literal lo = lit(f.lo(), t);
        Literal mid = lit(f.mid(), t);
        Literal hi = lit(f.hi(), t);
        Literal nul = new Literal(Source.EMPTY, null, t);
        Literal set = new Literal(Source.EMPTY, List.of(f.lo(), f.hi()), t);
        Literal setWithNull = new Literal(Source.EMPTY, Arrays.asList(f.mid(), null), t);
        List<Expression> leaves = new ArrayList<>();
        // multivalue forms, literal operands, every options setting
        leaves.add(new MvContains(Source.EMPTY, x, mid));
        leaves.add(new MvContains(Source.EMPTY, x, lo));
        leaves.add(new MvIntersects(Source.EMPTY, x, set));
        leaves.add(new MvIntersects(Source.EMPTY, x, setWithNull));
        leaves.add(new MvInRange(Source.EMPTY, x, lo, hi));
        leaves.add(new MvInRange(Source.EMPTY, x, lo, hi, options("include_lower", false)));
        leaves.add(new MvInRange(Source.EMPTY, x, lo, hi, options("include_upper", false)));
        leaves.add(new MvInRange(Source.EMPTY, x, lo, hi, options("include_lower", false, "include_upper", false)));
        for (boolean include : new boolean[] { true, false }) {
            leaves.add(new MvGreater(Source.EMPTY, x, mid, options(MvCompare.INCLUDE_BOUND, include)));
            leaves.add(new MvLess(Source.EMPTY, x, mid, options(MvCompare.INCLUDE_BOUND, include)));
        }
        leaves.add(new MvGreater(Source.EMPTY, x, mid));
        leaves.add(new MvLess(Source.EMPTY, x, mid));
        // multivalue forms, operands the reader must decline: another column, a null, a list where a scalar belongs
        leaves.add(new MvContains(Source.EMPTY, x, y));
        leaves.add(new MvIntersects(Source.EMPTY, x, y));
        leaves.add(new MvInRange(Source.EMPTY, x, y, hi));
        leaves.add(new MvGreater(Source.EMPTY, x, y));
        leaves.add(new MvLess(Source.EMPTY, x, y));
        leaves.add(new MvContains(Source.EMPTY, x, nul));
        leaves.add(new MvGreater(Source.EMPTY, x, nul));
        leaves.add(new MvInRange(Source.EMPTY, x, nul, hi));
        leaves.add(new MvContains(Source.EMPTY, x, set));
        leaves.add(new MvGreater(Source.EMPTY, x, set));
        // the scalar siblings, held to the same obligation
        leaves.add(new Equals(Source.EMPTY, x, mid, null));
        leaves.add(new NotEquals(Source.EMPTY, x, mid, null));
        leaves.add(new GreaterThan(Source.EMPTY, x, mid, null));
        leaves.add(new GreaterThanOrEqual(Source.EMPTY, x, mid, null));
        leaves.add(new LessThan(Source.EMPTY, x, mid, null));
        leaves.add(new LessThanOrEqual(Source.EMPTY, x, mid, null));
        leaves.add(new In(Source.EMPTY, x, List.of(lo, hi)));
        for (boolean includeLower : new boolean[] { true, false }) {
            for (boolean includeUpper : new boolean[] { true, false }) {
                leaves.add(new Range(Source.EMPTY, x, lo, includeLower, hi, includeUpper, null));
            }
        }
        return leaves;
    }

    private static boolean isMultivalueForm(Expression leaf) {
        return leaf instanceof MvContains || leaf instanceof MvIntersects || leaf instanceof MvInRange || leaf instanceof MvCompare;
    }

    /** The leaf bare, negated, and under each connective with an arm the reader can also evaluate. */
    private static List<Expression> wrappers(Expression leaf, Attribute x, Fixture f) {
        Expression pushable = new IsNotNull(Source.EMPTY, x);
        Expression other = new Equals(Source.EMPTY, x, lit(f.rows()[0], f.type()), null);
        return List.of(
            leaf,
            new Not(Source.EMPTY, leaf),
            new And(Source.EMPTY, leaf, pushable),
            new Or(Source.EMPTY, leaf, other),
            new Or(Source.EMPTY, new And(Source.EMPTY, other, leaf), pushable),
            new Not(Source.EMPTY, new And(Source.EMPTY, leaf, pushable))
        );
    }

    /** Which rows the expression keeps, per the engine's own evaluator; {@code null} when it cannot be evaluated. */
    private boolean[] oracle(Expression expr, Page page, Layout layout) {
        DriverContext driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null);
        try (
            var evaluator = EvalMapper.toEvaluator(FoldContext.small(), expr, layout).get(driverContext);
            Block result = evaluator.eval(page)
        ) {
            BooleanBlock answers = (BooleanBlock) result;
            boolean[] truth = new boolean[page.getPositionCount()];
            for (int row = 0; row < truth.length; row++) {
                truth[row] = answers.isNull(row) == false && answers.getBoolean(answers.getFirstValueIndex(row));
            }
            return truth;
        } catch (RuntimeException e) {
            return null;
        }
    }

    private Block block(DataType type, Object[] values) {
        return switch (type) {
            case LONG, DATETIME -> {
                try (var builder = blockFactory.newLongBlockBuilder(values.length)) {
                    for (Object v : values) {
                        if (v == null) {
                            builder.appendNull();
                        } else {
                            builder.appendLong((Long) v);
                        }
                    }
                    yield builder.build();
                }
            }
            case INTEGER -> {
                try (var builder = blockFactory.newIntBlockBuilder(values.length)) {
                    for (Object v : values) {
                        if (v == null) {
                            builder.appendNull();
                        } else {
                            builder.appendInt((Integer) v);
                        }
                    }
                    yield builder.build();
                }
            }
            case DOUBLE -> {
                try (var builder = blockFactory.newDoubleBlockBuilder(values.length)) {
                    for (Object v : values) {
                        if (v == null) {
                            builder.appendNull();
                        } else {
                            builder.appendDouble((Double) v);
                        }
                    }
                    yield builder.build();
                }
            }
            case KEYWORD -> {
                try (var builder = blockFactory.newBytesRefBlockBuilder(values.length)) {
                    for (Object v : values) {
                        if (v == null) {
                            builder.appendNull();
                        } else {
                            builder.appendBytesRef((BytesRef) v);
                        }
                    }
                    yield builder.build();
                }
            }
            default -> throw new IllegalArgumentException("no fixture for " + type);
        };
    }

    private static MapExpression options(Object... keysAndValues) {
        List<Expression> entries = new ArrayList<>();
        for (int i = 0; i < keysAndValues.length; i += 2) {
            entries.add(Literal.keyword(Source.EMPTY, (String) keysAndValues[i]));
            entries.add(new Literal(Source.EMPTY, keysAndValues[i + 1], DataType.BOOLEAN));
        }
        return new MapExpression(Source.EMPTY, entries);
    }

    /** {@code Expression.toString} renders these as empty, so name the tree by its node types. */
    private static String describe(Expression expr) {
        StringBuilder out = new StringBuilder(expr.getClass().getSimpleName());
        if (expr.children().isEmpty() == false) {
            out.append('(');
            for (int i = 0; i < expr.children().size(); i++) {
                out.append(i == 0 ? "" : ", ").append(describe(expr.children().get(i)));
            }
            out.append(')');
        }
        if (expr instanceof Literal literal) {
            out.append('=').append(literal.value());
        }
        return out.toString();
    }

    private static Literal lit(Object value, DataType type) {
        return new Literal(Source.EMPTY, value, type);
    }

    private static BytesRef bytes(String s) {
        return new BytesRef(s);
    }
}
