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
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedStar;
import org.elasticsearch.xpack.esql.plan.logical.Highlight;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.highlight.HighlightSupport;

import java.util.List;

/**
 * Fills implicit HIGHLIGHT query and ON fields during analysis so generated columns exist for later KEEP. This has to
 * settle during analysis because those columns are part of {@link Highlight#output()}, so a downstream
 * {@code KEEP highlight_title} can only resolve once they exist. Deriving nothing leaves the node untouched and lets
 * {@code Highlight#postAnalysisVerification} report the failure the user can act on.
 * <p>
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
            query = HighlightSupport.collectImplicitQuery(highlight.child(), highlight.source()).query();
            implicit = query != null;
        }

        List<NamedExpression> fields = highlight.fields();
        List<Attribute> generated = highlight.generatedAttributes();
        boolean star = fields.size() == 1 && fields.getFirst() instanceof UnresolvedStar;
        if (star || (fields.isEmpty() && query != null && query.resolved())) {
            List<Attribute> childOutput = highlight.child().output();
            // A derived query is not held to the field types an explicit one is: it was borrowed from an upstream WHERE
            // that may legitimately search non-text fields, and deriveFields already drops those names. Highlighting the
            // text fields that remain beats rejecting a query the user never wrote on this command. Same explicit-strict,
            // implicit-lenient split as the ON-membership check in Highlight#verifyQuery.
            String unhighlightable = star || implicit ? null : HighlightSupport.unhighlightableQueryField(query, childOutput);
            if (unhighlightable != null) {
                // The query names a concrete field that is not text/keyword (a missing one would have failed query
                // resolution). Report it through the unresolved-attribute channel so Verifier#checkUnresolvedAttributes
                // points at that field, instead of the generic "found no fields to highlight". Same idiom as the
                // Analyzer's Enrich/Lookup failures: maybeResolveAttribute leaves a custom-message UnresolvedAttribute
                // untouched, so it survives to the Verifier and is not rewritten to "Unknown column".
                fields = List.of(
                    new UnresolvedAttribute(
                        highlight.source(),
                        unhighlightable,
                        "HIGHLIGHT query field ["
                            + unhighlightable
                            + "] is not a text or keyword field of the input; add an explicit ON clause or remove it from the query"
                    )
                );
            } else {
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
        }

        if (query == highlight.query() && fields == highlight.fields()) {
            return highlight;
        }
        return highlight.withResolved(query, implicit, fields, generated);
    }
}
