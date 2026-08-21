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
 * Fills in the parts of {@code HIGHLIGHT} the user left out: the query, taken from full-text predicates in upstream {@code WHERE}
 * commands, and the highlighted columns, which are either the fields the query names or - for {@code ON *} - every text/keyword column
 * reaching the command. Both have to be settled during analysis because the generated {@code <prefix><field>} columns are part of
 * {@link Highlight#output()}, so a downstream {@code KEEP highlight_title} can only resolve once they exist.
 * <p>
 * This is a rule of its own, with {@link #skipResolved()} disabled, because {@code WHERE <full-text> | HIGHLIGHT ON <fields>} still needs
 * a query derived even though every one of its expressions is resolved: a rule that skips resolved plans would never visit it. The forms
 * with no {@code ON} list are visited regardless, since {@link Highlight#expressionsResolved()} treats an empty field list as unsettled.
 * <p>
 * Deriving nothing leaves the node untouched and lets {@link Highlight#postAnalysisVerification} report the failure the user can act on.
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
        List<Attribute> childOutput = highlight.child().output();

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
            List<NamedExpression> derived = star
                ? HighlightSupport.allHighlightableFields(childOutput)
                : HighlightSupport.deriveFields(query, childOutput);
            if (derived.isEmpty() == false) {
                fields = derived;
                // Mints fresh NameIds, so it has to stay inside this branch; calling it on every pass would never reach a fixpoint.
                generated = Highlight.generatedAttributesFor(highlight.source(), highlight.prefix(), fields);
            } else if (star) {
                // Replace the star: left in place it reports "Cannot determine columns for [*]", which the Verifier hits first and
                // which masks the "found no text or keyword fields" message that actually tells the user what to do.
                fields = List.of();
            }
        }

        // These identity checks look like the termination guard, but `fields` is re-read from `highlight` at line 59,
        // so they trivially hold unless the branch above reassigned it. The real guard is `star || fields.isEmpty()`
        // clearing itself once fields are derived (or replaced with `List.of()`), so the next pass takes this path.
        if (query == highlight.query() && fields == highlight.fields()) {
            return highlight;
        }
        return highlight.withResolved(query, implicit, fields, generated);
    }

    /**
     * Collects the searchable conjuncts of every {@code WHERE} that still describes the documents reaching {@code HIGHLIGHT}, OR-ing
     * them together. The walk moves down the child chain - children are upstream commands - and stops at the first node that is not
     * {@link DocPreserving}, because past that point a row no longer maps to a single document and its predicates say nothing about
     * what to highlight.
     * <p>
     * Predicates are collected as-is, with no check that their attributes are still live: a predicate whose field was later dropped or
     * renamed translates against a context that only knows the ON fields, so it becomes a match-none query and the column comes out
     * null. That is the documented behavior. An {@code AttributeSet} liveness guard would be worse than useless here, since membership
     * is {@code NameId}-based and RENAME or MV_EXPAND mint fresh ids, silently dropping predicates.
     * <p>
     * Successive {@code WHERE}s narrow rows conjunctively, but their full-text conjuncts are OR-ed here for
     * highlighting, deliberately: the highlight query is "what might be relevant to show", not "what selected these
     * rows". By the time a row reaches {@code HIGHLIGHT} it already satisfied every {@code WHERE}, so per-field term
     * extraction produces the same snippets either way; OR is chosen because it is the more useful contract if the
     * highlight query itself ever becomes user-visible (e.g. via {@code EXPLAIN}).
     */
    private static Expression collectImplicitQuery(LogicalPlan child) {
        List<Expression> predicates = new ArrayList<>();
        LogicalPlan current = child;
        while (current instanceof DocPreserving && current instanceof UnaryPlan unary) {
            if (current instanceof Filter filter) {
                for (Expression conjunct : Predicates.splitAnd(filter.condition())) {
                    if (HighlightSupport.isSupportedImplicitPredicate(conjunct)) {
                        predicates.add(conjunct);
                    }
                }
            }
            current = unary.child();
        }
        return predicates.isEmpty() ? null : Predicates.combineOr(predicates);
    }
}
