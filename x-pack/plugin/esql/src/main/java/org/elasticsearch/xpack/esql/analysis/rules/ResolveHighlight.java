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
        if (query == null) {
            query = HighlightSupport.collectImplicitQuery(highlight.child(), highlight.source()).query();
            implicit = query != null;
        }

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

        MapExpression options = withUniformAnalyzer(highlight.options(), query, highlight.source());
        if (query == highlight.query() && fields == highlight.fields() && options == highlight.options()) {
            return highlight;
        }
        Highlight updated = highlight.withResolved(query, implicit, fields, generated);
        return options == highlight.options() ? updated : updated.withOptions(options);
    }

    /**
     * Copies a uniform leaf analyzer into WITH when unset. Disagreement is left for verification.
     */
    private static MapExpression withUniformAnalyzer(MapExpression options, Expression query, Source source) {
        if (query == null || query.resolved() == false || (options != null && options.get(Highlight.ANALYZER) != null)) {
            return options;
        }
        String uniform = HighlightSupport.uniformAnalyzerOf(query);
        if (uniform == null) {
            return options;
        }
        List<Expression> entries = new ArrayList<>();
        if (options != null) {
            entries.addAll(options.children());
        }
        entries.add(Literal.keyword(source, Highlight.ANALYZER));
        entries.add(Literal.keyword(source, uniform));
        return new MapExpression(options != null ? options.source() : source, entries);
    }
}
