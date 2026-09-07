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
 * Derives the HIGHLIGHT columns the user left implicit during analysis so the generated {@code <prefix><field>}
 * columns exist for a later KEEP: the fields an explicit query names, or - for {@code ON *} - every text/keyword
 * column reaching the command. This has to settle during analysis because those columns are part of
 * {@link Highlight#output()}, so a downstream {@code KEEP highlight_title} can only resolve once they exist.
 * <p>
 * The bare and {@code ON *} forms are visited because {@link Highlight#expressionsResolved()} treats an empty field
 * list as unsettled. Deriving nothing leaves the node untouched and lets {@link Highlight#postAnalysisVerification}
 * report the failure the user can act on.
 */
public class ResolveHighlight extends AnalyzerRule<Highlight> {

    @Override
    protected LogicalPlan rule(Highlight highlight) {
        if (highlight.childrenResolved() == false) {
            return highlight;
        }

        Expression query = highlight.query();
        List<NamedExpression> fields = highlight.fields();
        List<Attribute> generated = highlight.generatedAttributes();
        boolean star = fields.size() == 1 && fields.getFirst() instanceof UnresolvedStar;
        if (star || (fields.isEmpty() && query != null && query.resolved())) {
            List<Attribute> childOutput = highlight.child().output();
            String unhighlightable = star ? null : HighlightSupport.unhighlightableQueryField(query, childOutput);
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
        return highlight.withResolved(query, highlight.implicitQuery(), fields, generated);
    }
}
