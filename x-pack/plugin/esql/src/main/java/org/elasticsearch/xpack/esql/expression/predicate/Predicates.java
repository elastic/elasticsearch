/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.predicate;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public abstract class Predicates {

    public static List<Expression> splitAnd(Expression exp) {
        if (exp instanceof And and) {
            List<Expression> list = new ArrayList<>();
            list.addAll(splitAnd(and.left()));
            list.addAll(splitAnd(and.right()));
            return list;
        }
        return singletonList(exp);
    }

    public static List<Expression> splitOr(Expression exp) {
        if (exp instanceof Or or) {
            List<Expression> list = new ArrayList<>();
            list.addAll(splitOr(or.left()));
            list.addAll(splitOr(or.right()));
            return list;
        }
        return singletonList(exp);
    }

    public static Expression combineOr(List<Expression> exps) {
        return combine(exps, (l, r) -> new Or(l.source(), l, r));
    }

    public static Expression combineAnd(List<Expression> exps) {
        return combine(exps, (l, r) -> new And(l.source(), l, r));
    }

    /**
     * Build a binary 'pyramid' from the given list:
     * <pre>
     *       AND
     *      /   \
     *   AND     AND
     *  /   \   /   \
     * A     B C     D
     * </pre>
     *
     * using the given combiner.
     *
     * While a bit longer, this method creates a balanced tree as opposed to a plain
     * recursive approach which creates an unbalanced one (either to the left or right).
     */
    private static Expression combine(List<Expression> exps, BiFunction<Expression, Expression, Expression> combiner) {
        if (exps.isEmpty()) {
            return null;
        }

        // clone the list (to modify it)
        List<Expression> result = new ArrayList<>(exps);

        while (result.size() > 1) {
            // combine (in place) expressions in pairs
            // NB: this loop modifies the list (just like an array)
            for (int i = 0; i < result.size() - 1; i++) {
                // keep the current element to update it in place
                Expression l = result.get(i);
                // remove the next element due to combining
                Expression r = result.remove(i + 1);
                result.set(i, combiner.apply(l, r));
            }
        }

        return result.get(0);
    }

    public static List<Expression> inCommon(List<Expression> l, List<Expression> r) {
        List<Expression> common = new ArrayList<>(Math.min(l.size(), r.size()));
        for (Expression lExp : l) {
            for (Expression rExp : r) {
                if (lExp.semanticEquals(rExp)) {
                    common.add(lExp);
                }
            }
        }
        return common.isEmpty() ? emptyList() : common;
    }

    public static List<Expression> subtract(List<Expression> from, List<Expression> list) {
        List<Expression> diff = new ArrayList<>(Math.min(from.size(), list.size()));
        for (Expression f : from) {
            boolean found = false;
            for (Expression l : list) {
                if (f.semanticEquals(l)) {
                    found = true;
                    break;
                }
            }
            if (found == false) {
                diff.add(f);
            }
        }
        return diff.isEmpty() ? emptyList() : diff;
    }

    /**
     * Given a list of expressions of predicates, extract a new expression of
     * all the common ones and return it, along the original list with the
     * common ones removed.
     * <p>
     * Example: for ['field1 > 0 AND field2 > 0', 'field1 > 0 AND field3 > 0',
     * 'field1 > 0'], the function will return 'field1 > 0' as the common
     * predicate expression and ['field2 > 0', 'field3 > 0', Literal.TRUE] as
     * the left predicates list.
     *
     * @param expressions list of expressions to extract common predicates from.
     * @return a tuple having as the first element an expression of the common
     * predicates and as the second element the list of expressions with the
     * common predicates removed. If there are no common predicates, `null` will
     * be returned as the first element and the original list as the second. If
     * for one of the expressions in the input list, nothing is left after
     * trimming the common predicates, it will be replaced with Literal.TRUE.
     */
    public static Tuple<Expression, List<Expression>> extractCommon(List<Expression> expressions) {
        List<Expression> common = null;
        List<List<Expression>> splitAnds = new ArrayList<>(expressions.size());
        for (var expression : expressions) {
            var split = splitAnd(expression);
            common = common == null ? split : inCommon(split, common);
            if (common.isEmpty()) {
                return Tuple.tuple(null, expressions);
            }
            splitAnds.add(split);
        }

        List<Expression> trimmed = new ArrayList<>(expressions.size());
        final List<Expression> finalCommon = common;
        splitAnds.forEach(split -> {
            var subtracted = subtract(split, finalCommon);
            trimmed.add(subtracted.isEmpty() ? Literal.TRUE : combineAnd(subtracted));
        });
        return Tuple.tuple(combineAnd(common), trimmed);
    }
}
