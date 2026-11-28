/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.predicate;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.planner.Layout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.IntPredicate;

/**
 * Three-valued composition of a {@link PredicateExpr}. {@code AND} and {@code OR} evaluate the left side, then the right
 * side on the remaining positions, in that order. A later pass must not reorder them: an expression can throw or warn,
 * and the row that does so has to stay stable. A source refiner is used only when the source can evaluate that leaf
 * exactly in place. Otherwise the engine loads the selected columns as blocks and evaluates the leaf with the ordinary
 * {@link ExpressionEvaluator}.
 */
public final class Predicates {
    private static final int FALSE = 0;
    private static final int TRUE = 1;
    private static final int NULL = 2;

    private Predicates() {}

    public static RowPredicate compile(PredicateExpr predicate, PredicateSource source, DriverContext context) {
        return switch (predicate) {
            case PredicateExpr.And and -> new And(
                context.blockFactory(),
                compile(and.left(), source, context),
                compile(and.right(), source, context)
            );
            case PredicateExpr.Or or -> new Or(
                context.blockFactory(),
                compile(or.left(), source, context),
                compile(or.right(), source, context)
            );
            case PredicateExpr.Not not -> new Not(context.blockFactory(), compile(not.inner(), source, context));
            case PredicateExpr.FieldEquals equals -> fieldEquals(equals, source, context);
            case PredicateExpr.ProductGreaterThan product -> productGreaterThan(product, context);
        };
    }

    public static RowPredicate alwaysTrue(DriverContext context) {
        BlockFactory factory = context.blockFactory();
        return (batch, selection) -> factory.newConstantBooleanBlockWith(true, selection.length);
    }

    static RowPredicate and(RowPredicate left, RowPredicate right, DriverContext context) {
        return new And(context.blockFactory(), left, right);
    }

    private static RowPredicate fieldEquals(PredicateExpr.FieldEquals equals, PredicateSource source, DriverContext context) {
        if (source.analyze(equals).evaluatesThreeValued()) {
            PredicateRefiner refiner = source.refiner(equals).orElseThrow(() -> new IllegalStateException("missing refiner for " + equals));
            return refiner::evaluate;
        }
        FieldAttribute field = attribute(new FieldRef(equals.field(), DataType.KEYWORD));
        Expression expression = new Equals(
            Source.EMPTY,
            field,
            new Literal(Source.EMPTY, new BytesRef(equals.value()), DataType.KEYWORD),
            null
        );
        return new Evaluated(List.of(field), expression, context);
    }

    private static RowPredicate productGreaterThan(PredicateExpr.ProductGreaterThan product, DriverContext context) {
        FieldAttribute left = attribute(new FieldRef(product.left(), DataType.LONG));
        FieldAttribute right = product.left().equals(product.right()) ? left : attribute(new FieldRef(product.right(), DataType.LONG));
        Expression expression = new GreaterThan(
            Source.EMPTY,
            new Mul(Source.EMPTY, left, right),
            new Literal(Source.EMPTY, product.threshold(), DataType.LONG),
            null
        );
        return new Evaluated(left == right ? List.of(left) : List.of(left, right), expression, context);
    }

    private static FieldAttribute attribute(FieldRef field) {
        return new FieldAttribute(
            Source.EMPTY,
            field.name(),
            new EsField(field.name(), field.type(), Collections.emptyMap(), false, EsField.TimeSeriesFieldType.NONE)
        );
    }

    private static int state(BooleanBlock block, int position) {
        if (block.isNull(position)) {
            return NULL;
        }
        return block.getBoolean(block.getFirstValueIndex(position)) ? TRUE : FALSE;
    }

    private static void append(BooleanBlock.Builder builder, int state) {
        switch (state) {
            case TRUE -> builder.appendBoolean(true);
            case FALSE -> builder.appendBoolean(false);
            case NULL -> builder.appendNull();
            default -> throw new IllegalArgumentException("unknown state [" + state + "]");
        }
    }

    /** The selected batch positions whose result state passes {@code keep}, in selection order. */
    private static int[] selected(int[] selection, BooleanBlock result, IntPredicate keep) {
        int[] kept = new int[selection.length];
        int count = 0;
        for (int i = 0; i < selection.length; i++) {
            if (keep.test(state(result, i))) {
                kept[count++] = selection[i];
            }
        }
        return count == kept.length ? kept : Arrays.copyOf(kept, count);
    }

    private static BooleanBlock evaluateOrEmpty(RowPredicate predicate, SelectionBatch batch, int[] selection, BlockFactory factory) {
        return selection.length == 0 ? factory.newConstantBooleanBlockWith(false, 0) : predicate.evaluate(batch, selection);
    }

    private static final class Evaluated implements RowPredicate {
        private final List<FieldRef> fields = new ArrayList<>();
        private final ExpressionEvaluator evaluator;

        private Evaluated(List<FieldAttribute> attributes, Expression expression, DriverContext context) {
            Layout.Builder layout = new Layout.Builder();
            for (FieldAttribute attribute : attributes) {
                layout.append(attribute);
                fields.add(new FieldRef(attribute.fieldName().string(), attribute.dataType()));
            }
            this.evaluator = EvalMapper.toEvaluator(FoldContext.small(), expression, layout.build()).get(context);
        }

        @Override
        public BooleanBlock evaluate(SelectionBatch batch, int[] selection) {
            Block[] blocks = new Block[fields.size()];
            boolean loaded = false;
            try {
                for (int i = 0; i < blocks.length; i++) {
                    blocks[i] = batch.load(fields.get(i), selection);
                }
                loaded = true;
            } finally {
                if (loaded == false) {
                    Releasables.closeWhileHandlingException(blocks);
                }
            }
            try (Page page = new Page(selection.length, blocks)) {
                return (BooleanBlock) evaluator.eval(page);
            }
        }

        @Override
        public void close() {
            evaluator.close();
        }
    }

    private static final class And implements RowPredicate {
        private final BlockFactory factory;
        private final RowPredicate left;
        private final RowPredicate right;

        private And(BlockFactory factory, RowPredicate left, RowPredicate right) {
            this.factory = factory;
            this.left = left;
            this.right = right;
        }

        @Override
        public BooleanBlock evaluate(SelectionBatch batch, int[] selection) {
            if (selection.length == 0) {
                return factory.newConstantBooleanBlockWith(false, 0);
            }
            try (BooleanBlock leftResult = left.evaluate(batch, selection)) {
                // False on the left decides AND. Null still needs the right side, because a false there changes the result.
                int[] pending = selected(selection, leftResult, state -> state != FALSE);
                try (
                    BooleanBlock rightResult = evaluateOrEmpty(right, batch, pending, factory);
                    BooleanBlock.Builder builder = factory.newBooleanBlockBuilder(selection.length)
                ) {
                    int next = 0;
                    for (int i = 0; i < selection.length; i++) {
                        int leftState = state(leftResult, i);
                        if (leftState == FALSE) {
                            builder.appendBoolean(false);
                            continue;
                        }
                        int rightState = state(rightResult, next++);
                        if (rightState == FALSE) {
                            append(builder, FALSE);
                        } else if (leftState == NULL || rightState == NULL) {
                            append(builder, NULL);
                        } else {
                            append(builder, TRUE);
                        }
                    }
                    return builder.build();
                }
            }
        }

        @Override
        public void close() {
            Releasables.close(left, right);
        }
    }

    private static final class Or implements RowPredicate {
        private final BlockFactory factory;
        private final RowPredicate left;
        private final RowPredicate right;

        private Or(BlockFactory factory, RowPredicate left, RowPredicate right) {
            this.factory = factory;
            this.left = left;
            this.right = right;
        }

        @Override
        public BooleanBlock evaluate(SelectionBatch batch, int[] selection) {
            if (selection.length == 0) {
                return factory.newConstantBooleanBlockWith(false, 0);
            }
            try (BooleanBlock leftResult = left.evaluate(batch, selection)) {
                // True on the left decides OR. Null still needs the right side.
                int[] pending = selected(selection, leftResult, state -> state != TRUE);
                try (
                    BooleanBlock rightResult = evaluateOrEmpty(right, batch, pending, factory);
                    BooleanBlock.Builder builder = factory.newBooleanBlockBuilder(selection.length)
                ) {
                    int next = 0;
                    for (int i = 0; i < selection.length; i++) {
                        int leftState = state(leftResult, i);
                        if (leftState == TRUE) {
                            append(builder, TRUE);
                            continue;
                        }
                        int rightState = state(rightResult, next++);
                        if (rightState == TRUE) {
                            append(builder, TRUE);
                        } else if (leftState == NULL || rightState == NULL) {
                            append(builder, NULL);
                        } else {
                            append(builder, FALSE);
                        }
                    }
                    return builder.build();
                }
            }
        }

        @Override
        public void close() {
            Releasables.close(left, right);
        }
    }

    private static final class Not implements RowPredicate {
        private final BlockFactory factory;
        private final RowPredicate inner;

        private Not(BlockFactory factory, RowPredicate inner) {
            this.factory = factory;
            this.inner = inner;
        }

        @Override
        public BooleanBlock evaluate(SelectionBatch batch, int[] selection) {
            if (selection.length == 0) {
                return factory.newConstantBooleanBlockWith(false, 0);
            }
            try (
                BooleanBlock result = inner.evaluate(batch, selection);
                BooleanBlock.Builder builder = factory.newBooleanBlockBuilder(selection.length)
            ) {
                for (int i = 0; i < selection.length; i++) {
                    int state = state(result, i);
                    append(builder, state == NULL ? NULL : state == TRUE ? FALSE : TRUE);
                }
                return builder.build();
            }
        }

        @Override
        public void close() {
            inner.close();
        }
    }
}
