/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis.rules;

import org.elasticsearch.xpack.esql.analysis.AnalyzerRules.AnalyzerRule;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.EntryExpression;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedStar;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.Highlight;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.highlight.HighlightSupport;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Derives implicit HIGHLIGHT query and ON fields so generated columns exist for KEEP.
 * {@link #skipResolved()} is false: {@code WHERE ... | HIGHLIGHT ON ...} is already resolved.
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
        Highlight.AnalyzerProvenance analyzerProvenance = highlight.analyzerProvenance();
        MapExpression options = highlight.options();
        String derivedAnalyzer = null;
        if (query == null) {
            HighlightSupport.ImplicitQuery borrowed = HighlightSupport.collectImplicitQuery(highlight.child(), highlight.source());
            query = borrowed.query();
            implicit = query != null;
            derivedAnalyzer = borrowed.analyzerName();
        } else if (implicit == false) {
            // Implicit queries already synthesized an analyzer on the pass that derived them.
            derivedAnalyzer = HighlightSupport.uniformAnalyzerOf(query);
        }
        MapExpression withAnalyzer = withDerivedAnalyzer(highlight.source(), options, derivedAnalyzer);
        // withDerivedAnalyzer returns the same instance when it adds nothing because the user already set one.
        // A new instance means we synthesized the analyzer from WHERE. Track that so verification uses the right error.
        if (withAnalyzer != options) {
            analyzerProvenance = Highlight.AnalyzerProvenance.DERIVED_FROM_WHERE;
        }
        options = withAnalyzer;

        List<NamedExpression> fields = highlight.fields();
        List<Attribute> generated = highlight.generatedAttributes();
        boolean star = fields.size() == 1 && fields.getFirst() instanceof UnresolvedStar;
        if (star || (fields.isEmpty() && query != null && query.resolved())) {
            List<Attribute> childOutput = highlight.child().output();
            String unhighlightable = star || implicit ? null : HighlightSupport.unhighlightableQueryField(query, childOutput);
            if (unhighlightable != null) {
                // UnresolvedAttribute so Verifier names this field instead of rewriting to "Unknown column".
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

        if (query == highlight.query()
            && fields == highlight.fields()
            && Objects.equals(options, highlight.options())
            && analyzerProvenance == highlight.analyzerProvenance()) {
            return highlight;
        }
        return highlight.withResolved(query, implicit, analyzerProvenance, fields, generated, options);
    }

    /**
     * Adds a derived analyzer unless the user already set one. Returns {@code options} unchanged when there is
     * nothing to add, so the caller's convergence check sees no change.
     */
    private static MapExpression withDerivedAnalyzer(Source source, MapExpression options, String derivedAnalyzer) {
        if (derivedAnalyzer == null || (options != null && options.containsKey(Highlight.ANALYZER))) {
            return options;
        }
        List<Expression> entries = new ArrayList<>();
        if (options != null) {
            for (EntryExpression entry : options.entryExpressions()) {
                entries.add(entry.key());
                entries.add(entry.value());
            }
        }
        entries.add(Literal.keyword(source, Highlight.ANALYZER));
        entries.add(Literal.keyword(source, derivedAnalyzer));
        return new MapExpression(source, entries);
    }
}
