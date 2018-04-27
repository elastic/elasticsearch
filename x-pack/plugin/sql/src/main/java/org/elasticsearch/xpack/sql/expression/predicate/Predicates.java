/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class Predicates {

    public static List<Expression> splitAnd(Expression exp) {
        if (exp instanceof And) {
            And and = (And) exp;
            List<Expression> list = new ArrayList<>();
            list.addAll(splitAnd(and.left()));
            list.addAll(splitAnd(and.right()));
            return list;
        }
        return Collections.singletonList(exp);
    }

    public static List<Expression> splitOr(Expression exp) {
        if (exp instanceof Or) {
            Or or = (Or) exp;
            List<Expression> list = new ArrayList<>();
            list.addAll(splitOr(or.left()));
            list.addAll(splitOr(or.right()));
            return list;
        }
        return Collections.singletonList(exp);
    }

    public static Expression combineOr(List<Expression> exps) {
        return exps.stream().reduce((l, r) -> new Or(l.location(), l, r)).orElse(null);
    }

    public static Expression combineAnd(List<Expression> exps) {
        return exps.stream().reduce((l, r) -> new And(l.location(), l, r)).orElse(null);
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
        return common.isEmpty() ? Collections.emptyList() : common;
    }

    public static List<Expression> subtract(List<Expression> from, List<Expression> r) {
        List<Expression> diff = new ArrayList<>(Math.min(from.size(), r.size()));
        for (Expression lExp : from) {
            for (Expression rExp : r) {
                if (!lExp.semanticEquals(rExp)) {
                    diff.add(lExp);
                }
            }
        }
        return diff.isEmpty() ? Collections.emptyList() : diff;
    }


    public static boolean canEvaluate(Expression exp, LogicalPlan plan) {
        return exp.references().subsetOf(plan.outputSet());
    }
}