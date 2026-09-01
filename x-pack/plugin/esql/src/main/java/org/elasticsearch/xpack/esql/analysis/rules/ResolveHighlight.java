/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis.rules;

import org.elasticsearch.xpack.esql.analysis.AnalyzerRules.AnalyzerRule;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedStar;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.plan.logical.DocPreserving;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Highlight;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.highlight.HighlightSupport;

import java.util.ArrayList;
import java.util.List;

/**
 * Fills implicit HIGHLIGHT query and ON fields during analysis so generated columns exist for later KEEP.
 * {@link #skipResolved()} is false because {@code WHERE <full-text> | HIGHLIGHT ON <fields>} is already resolved and
 * would otherwise be skipped.
 */
public class ResolveHighlight extends AnalyzerRule<Highlight> {

    @Override
    protected boolean skipResolved() {
        return false;
    }

    @Override
    protected LogicalPlan rule(Highlight highlight) {
        if (highlight.childrenResolved() == false) {
            return highlight;
        }

        Expression query = highlight.query();
        boolean implicit = highlight.implicitQuery();
        if (query == null) {
            query = collectImplicitQuery(highlight.child());
            implicit = query != null;
        }

        List<NamedExpression> fields = highlight.fields();
        List<Attribute> generated = highlight.generatedAttributes();
        boolean star = fields.size() == 1 && fields.getFirst() instanceof UnresolvedStar;
        if (star || (fields.isEmpty() && query != null && query.resolved())) {
            List<Attribute> childOutput = highlight.child().output();
            List<NamedExpression> derived = star
                ? HighlightSupport.allHighlightableFields(childOutput)
                : HighlightSupport.deriveFields(query, childOutput);
            if (derived.isEmpty() == false) {
                fields = derived;
                // generatedAttributesFor mints fresh NameIds; only call after fields actually change or analysis never converges.
                generated = Highlight.generatedAttributesFor(highlight.source(), highlight.prefix(), fields);
            } else if (star) {
                // Drop the star so Verifier reports "found no text or keyword fields" instead of "Cannot determine columns for [*]".
                fields = List.of();
            }
        }

        if (query == highlight.query() && fields == highlight.fields()) {
            return highlight;
        }
        return highlight.withResolved(query, implicit, fields, generated);
    }

    /**
     * Collects the searchable conjuncts of every {@code WHERE} that still describes the documents reaching {@code HIGHLIGHT}.
     * The walk moves down the child chain (children are upstream) and stops at the first node that is not
     * {@link DocPreserving}, because past that point a row no longer maps to a single document.
     * <p>
     * Predicates are collected as-is, with no check that their attributes are still live: a predicate whose field was later
     * dropped or renamed translates against a context that only knows the ON fields, so it becomes a match-none query
     * and the column comes out null. An {@code AttributeSet} liveness guard would be worse, since membership is
     * {@code NameId}-based and RENAME or MV_EXPAND mint fresh ids, silently dropping predicates.
     * <p>
     * WHEREs filter conjunctively, but highlight terms are OR-ed because the highlight query is display, not selection.
     */
    private static Expression collectImplicitQuery(LogicalPlan child) {
        List<Expression> predicates = new ArrayList<>();
        for (LogicalPlan current = child; current instanceof UnaryPlan unary && current instanceof DocPreserving; current = unary.child()) {
            if (current instanceof Filter filter) {
                for (Expression conjunct : Predicates.splitAnd(filter.condition())) {
                    if (HighlightSupport.isSupportedImplicitPredicate(conjunct)) {
                        predicates.add(conjunct);
                    }
                }
            }
        }
        return predicates.isEmpty() ? null : Predicates.combineOr(predicates);
    }
}
