/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.predicate;

/**
 * Three-valued composition of a {@link PredicateExpr}. {@code AND} and {@code OR} evaluate the left side, then the right
 * side on the remaining positions, in that order. A later pass must not reorder them: an expression can throw or warn,
 * and the row that does so has to stay stable. A source refiner is used only when the source can evaluate that leaf
 * exactly in place. Otherwise the engine loads the columns and evaluates the leaf itself.
 */
public final class Predicates {
    private Predicates() {}

    public static RowPredicate compile(PredicateExpr predicate, PredicateSource source) {
        return switch (predicate) {
            case PredicateExpr.And and -> new And(compile(and.left(), source), compile(and.right(), source));
            case PredicateExpr.Or or -> new Or(compile(or.left(), source), compile(or.right(), source));
            case PredicateExpr.Not not -> new Not(compile(not.inner(), source));
            case PredicateExpr.FieldEquals equals -> fieldEquals(equals, source);
            case PredicateExpr.ProductGreaterThan product -> new Product(product);
        };
    }

    public static RowPredicate alwaysTrue() {
        return (batch, candidates) -> PredicateResult.of(candidates, SelectionMask.none(batch.positionCount()));
    }

    static RowPredicate and(RowPredicate left, RowPredicate right) {
        return new And(left, right);
    }

    private static RowPredicate fieldEquals(PredicateExpr.FieldEquals equals, PredicateSource source) {
        if (source.analyze(equals).evaluatesThreeValued()) {
            PredicateRefiner refiner = source.refiner(equals).orElseThrow(() -> new IllegalStateException("missing refiner for " + equals));
            return (batch, candidates) -> refiner.evaluate(batch, candidates);
        }
        return new FieldComparison(equals);
    }

    private static final class And implements RowPredicate {
        private final RowPredicate left;
        private final RowPredicate right;

        private And(RowPredicate left, RowPredicate right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public PredicateResult evaluate(SelectionBatch batch, SelectionMask candidates) {
            if (candidates.isEmpty()) {
                return PredicateResult.none(batch.positionCount());
            }
            PredicateResult leftResult = left.evaluate(batch, candidates);
            // False on the left decides AND. Null still needs the right side, because a false there changes the result.
            SelectionMask rightCandidates = leftResult.trues().or(leftResult.nulls());
            if (rightCandidates.isEmpty()) {
                return PredicateResult.none(batch.positionCount());
            }
            PredicateResult rightResult = right.evaluate(batch, rightCandidates);
            SelectionMask trues = leftResult.trues().and(rightResult.trues());
            SelectionMask nulls = leftResult.trues()
                .and(rightResult.nulls())
                .or(leftResult.nulls().and(rightResult.trues()))
                .or(leftResult.nulls().and(rightResult.nulls()));
            return PredicateResult.of(trues, nulls);
        }
    }

    private static final class Or implements RowPredicate {
        private final RowPredicate left;
        private final RowPredicate right;

        private Or(RowPredicate left, RowPredicate right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public PredicateResult evaluate(SelectionBatch batch, SelectionMask candidates) {
            if (candidates.isEmpty()) {
                return PredicateResult.none(batch.positionCount());
            }
            PredicateResult leftResult = left.evaluate(batch, candidates);
            // True on the left decides OR. Null still needs the right side.
            SelectionMask rightCandidates = candidates.andNot(leftResult.trues());
            if (rightCandidates.isEmpty()) {
                return leftResult;
            }
            PredicateResult rightResult = right.evaluate(batch, rightCandidates);
            SelectionMask trues = leftResult.trues().or(rightResult.trues());
            SelectionMask nulls = leftResult.nulls().andNot(rightResult.trues()).or(rightResult.nulls().andNot(leftResult.trues()));
            return PredicateResult.of(trues, nulls);
        }
    }

    private static final class Not implements RowPredicate {
        private final RowPredicate inner;

        private Not(RowPredicate inner) {
            this.inner = inner;
        }

        @Override
        public PredicateResult evaluate(SelectionBatch batch, SelectionMask candidates) {
            if (candidates.isEmpty()) {
                return PredicateResult.none(batch.positionCount());
            }
            PredicateResult result = inner.evaluate(batch, candidates);
            SelectionMask trues = candidates.andNot(result.trues()).andNot(result.nulls());
            return PredicateResult.of(trues, result.nulls());
        }
    }

    private static final class FieldComparison implements RowPredicate {
        private final PredicateExpr.FieldEquals equals;

        private FieldComparison(PredicateExpr.FieldEquals equals) {
            this.equals = equals;
        }

        @Override
        public PredicateResult evaluate(SelectionBatch batch, SelectionMask candidates) {
            int[] positions = candidates.positions();
            boolean[] trues = new boolean[batch.positionCount()];
            boolean[] nulls = new boolean[batch.positionCount()];
            try (Column column = batch.load(new FieldRef(equals.field()), candidates)) {
                for (int ordinal = 0; ordinal < positions.length; ordinal++) {
                    Object value = column.value(ordinal);
                    int position = positions[ordinal];
                    if (value == null) {
                        nulls[position] = true;
                    } else if (equals.value().equals(value)) {
                        trues[position] = true;
                    }
                }
            }
            return PredicateResult.of(SelectionMask.of(trues), SelectionMask.of(nulls));
        }
    }

    private static final class Product implements RowPredicate {
        private final PredicateExpr.ProductGreaterThan product;

        private Product(PredicateExpr.ProductGreaterThan product) {
            this.product = product;
        }

        @Override
        public PredicateResult evaluate(SelectionBatch batch, SelectionMask candidates) {
            int[] positions = candidates.positions();
            boolean[] trues = new boolean[batch.positionCount()];
            boolean[] nulls = new boolean[batch.positionCount()];
            try (
                Column left = batch.load(new FieldRef(product.left()), candidates);
                Column right = batch.load(new FieldRef(product.right()), candidates)
            ) {
                for (int ordinal = 0; ordinal < positions.length; ordinal++) {
                    Object leftValue = left.value(ordinal);
                    Object rightValue = right.value(ordinal);
                    int position = positions[ordinal];
                    if (leftValue == null || rightValue == null) {
                        nulls[position] = true;
                    } else if (((Number) leftValue).longValue() * ((Number) rightValue).longValue() > product.threshold()) {
                        trues[position] = true;
                    }
                }
            }
            return PredicateResult.of(SelectionMask.of(trues), SelectionMask.of(nulls));
        }
    }
}
