/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.core.Nullable;

/**
 * An expression producing {@code text} values with a declared values (index-time) analyzer — the role the mapping's
 * {@code analyzer} plays for an indexed text field. Implemented by {@code TO_TEXT} (the declaration site) and by
 * {@link ReferenceAttribute} (which carries the declaration across {@code EVAL}/{@code RENAME} boundaries, see
 * {@link Alias#toAttribute}). Consumers and propagation sites discover the analyzer through
 * {@link #declaredValuesAnalyzerOf}, whichever form the expression takes.
 * <p>
 * TODO: the declared analyzer is really a refinement of {@link Expression#dataType()}. If text types could carry
 * parameters ({@code text(analyzer=...)}), the declaration would flow with the type through every place that mints
 * and serializes attributes, with no per-site propagation and no capability interface. Revisit if a second
 * text-column property (e.g. {@code similarity}) shows up.
 */
public interface AnalyzedTextExpression {

    /**
     * The analyzer that applies to a text column declaring none. Declaring it explicitly is therefore the same as
     * declaring nothing, which consumers comparing declarations have to treat as equal.
     */
    String STANDARD_ANALYZER = "standard";

    /**
     * The declared values analyzer name, or {@code null} when none was declared ({@link #STANDARD_ANALYZER} applies).
     */
    @Nullable
    String valuesAnalyzer();

    /**
     * The values analyzer {@code expression} declares, or {@code null} when it declares none. Prefer
     * {@link #effectiveValuesAnalyzerOf} when comparing two expressions, so that declaring the default explicitly
     * does not read as a difference.
     */
    @Nullable
    static String declaredValuesAnalyzerOf(Expression expression) {
        return expression instanceof AnalyzedTextExpression analyzed ? analyzed.valuesAnalyzer() : null;
    }

    /**
     * The values analyzer that applies to {@code expression}, naming {@link #STANDARD_ANALYZER} rather than returning
     * {@code null} when none was declared. Use this to compare declarations, so that an explicit {@code standard}
     * compares equal to no declaration at all.
     */
    static String effectiveValuesAnalyzerOf(Expression expression) {
        String declared = declaredValuesAnalyzerOf(expression);
        return declared == null ? STANDARD_ANALYZER : declared;
    }
}
