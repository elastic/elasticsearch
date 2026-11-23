/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.predicate;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Splits a predicate into a source candidate constraint and a residual the engine evaluates. A source may drive
 * candidate production only for a conjunct it can prove does not drop a matching row. A superset proof stays in the
 * residual so the engine still verifies each retained row. A disjunction is not split into one of its branches.
 */
public final class PredicatePlans {
    private PredicatePlans() {}

    public static FilterPlan filter(PredicateExpr predicate, PredicateSource source) {
        List<PredicateExpr> constraint = new ArrayList<>();
        List<PredicateExpr> residual = new ArrayList<>();
        for (PredicateExpr conjunct : flattenAnd(predicate)) {
            PredicateCapability capability = source.analyze(conjunct);
            if (capability.produces() && capability.support() == PredicateSupport.EXACT) {
                constraint.add(conjunct);
            } else if (capability.produces() && capability.support() == PredicateSupport.SUPERSET) {
                constraint.add(conjunct);
                residual.add(conjunct);
            } else {
                residual.add(conjunct);
            }
        }
        Projection projection = new Projection(fields(residual));
        RowPredicate evaluator = residual.isEmpty() ? Predicates.alwaysTrue() : compileAll(residual, source);
        return new FilterPlan(
            new CandidateConstraint(constraint),
            projection,
            source.open(new CandidateConstraint(constraint), projection),
            evaluator
        );
    }

    /** In-place evaluation. The input batch is kept, so false and null rows are still positions in the result. */
    public static EvalPlan eval(PredicateExpr predicate, PredicateSource source) {
        return new EvalPlan(Predicates.compile(predicate, source));
    }

    private static RowPredicate compileAll(List<PredicateExpr> conjuncts, PredicateSource source) {
        RowPredicate combined = Predicates.compile(conjuncts.get(0), source);
        for (int i = 1; i < conjuncts.size(); i++) {
            combined = Predicates.and(combined, Predicates.compile(conjuncts.get(i), source));
        }
        return combined;
    }

    private static List<PredicateExpr> flattenAnd(PredicateExpr predicate) {
        List<PredicateExpr> conjuncts = new ArrayList<>();
        flattenAnd(predicate, conjuncts);
        return conjuncts;
    }

    private static void flattenAnd(PredicateExpr predicate, List<PredicateExpr> conjuncts) {
        if (predicate instanceof PredicateExpr.And and) {
            flattenAnd(and.left(), conjuncts);
            flattenAnd(and.right(), conjuncts);
        } else {
            conjuncts.add(predicate);
        }
    }

    private static List<FieldRef> fields(List<PredicateExpr> predicates) {
        Set<FieldRef> fields = new LinkedHashSet<>();
        for (PredicateExpr predicate : predicates) {
            fields(predicate, fields);
        }
        return List.copyOf(fields);
    }

    private static void fields(PredicateExpr predicate, Set<FieldRef> fields) {
        switch (predicate) {
            case PredicateExpr.FieldEquals equals -> fields.add(new FieldRef(equals.field()));
            case PredicateExpr.ProductGreaterThan product -> {
                fields.add(new FieldRef(product.left()));
                fields.add(new FieldRef(product.right()));
            }
            case PredicateExpr.And and -> {
                fields(and.left(), fields);
                fields(and.right(), fields);
            }
            case PredicateExpr.Or or -> {
                fields(or.left(), fields);
                fields(or.right(), fields);
            }
            case PredicateExpr.Not not -> fields(not.inner(), fields);
        }
    }

}
