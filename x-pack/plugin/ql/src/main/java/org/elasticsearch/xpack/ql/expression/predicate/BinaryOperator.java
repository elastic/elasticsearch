/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.expression.predicate;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.SECOND;

/**
 * Operator is a specialized binary predicate where both sides have the compatible types
 * (it's up to the analyzer to do any conversion if needed).
 */
public abstract class BinaryOperator<T, U, R, F extends PredicateBiFunction<T, U, R>> extends BinaryPredicate<T, U, R, F> {

    protected BinaryOperator(Source source, Expression left, Expression right, F function) {
        super(source, left, right, function);
    }

    protected abstract TypeResolution resolveInputType(Expression e, ParamOrdinal paramOrdinal);

    public abstract BinaryOperator<T, U, R, F> swapLeftAndRight();

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = resolveInputType(left(), FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }
        return resolveInputType(right(), SECOND);
    }

    protected boolean isCommutative() {
        return false;
    }

    @Override
    protected Expression canonicalize() {
        // fast check
        if (isCommutative() == false) {
            Expression exp = left().semanticHash() > right().semanticHash() ? swapLeftAndRight() : this;
            // swap is not guaranteed to return a different expression, in which case simply delegate to super to avoid a cycle
            return exp != this ? exp.canonical() : super.canonicalize();
        }
        // break down all connected commutative operators
        // in order to sort all their children at once
        // then reassemble/reduce back the expression
        List<Expression> commutativeChildren = new ArrayList<>(2);
        collectCommutative(commutativeChildren, this);
        // sort
        commutativeChildren.sort((l, r) -> Integer.compare(l.semanticHash(), r.semanticHash()));

        // reduce all children using the current operator - this method creates a balanced tree
        while (commutativeChildren.size() > 1) {
            // combine (in place) expressions in pairs
            // NB: this loop modifies the list (just like an array)
            for (int i = 0; i < commutativeChildren.size() - 1; i++) {
                // reduce two children into one and moves to the next pair
                Expression current = commutativeChildren.get(i);
                Expression next = commutativeChildren.remove(i + 1);
                // do the update in place to minimize the amount of array modifications
                commutativeChildren.set(i, replaceChildren(current, next));

            }
        }
        Iterator<Expression> iterator = commutativeChildren.iterator();
        Expression last = iterator.next();
        while (iterator.hasNext()) {
            last = replaceChildren(last, iterator.next());
        }
        return last;
    }

    protected void collectCommutative(List<Expression> commutative, Expression expression) {
        // keep digging for same binary operator
        if (getClass() == expression.getClass()) {
            BinaryOperator<?, ?, ?, ?> bi = (BinaryOperator<?, ?, ?, ?>) expression;
            collectCommutative(commutative, bi.left());
            collectCommutative(commutative, bi.right());
        } else {
            // not same operation - no ordering possible
            commutative.add(expression.canonical());
        }
    }
}
