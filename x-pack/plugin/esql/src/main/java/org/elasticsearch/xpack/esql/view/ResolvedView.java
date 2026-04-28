/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

/**
 * Information about a view that was concretely resolved during view resolution.
 * <p>
 * The {@code indexPattern} field is a comma-separated index expression suitable for use as a
 * field-caps target. It includes the view name followed by any exclusion patterns that appeared
 * <em>after</em> the view's referencing position in the parent UR — exclusions earlier in the
 * pattern list do not apply, since index resolution processes patterns left-to-right and an
 * exclusion only narrows the indices accumulated up to that point.
 * <p>
 * Example: given {@code v1 = FROM v2,metrics*,-*2025}, when resolving {@code v1}'s body the parent
 * pattern is {@code v2,metrics*,-*2025}. The view {@code v2} appears at position 0, and the only
 * later exclusion is {@code -*2025}, so {@code v2}'s {@code indexPattern} is {@code "v2,-*2025"} —
 * matching the lenient call {@code field-caps-lenient(v2,-*2025)} from
 * <a href="https://github.com/elastic/esql-planning/issues/543">esql-planning#543</a>.
 * <p>
 * Position-aware example: given {@code FROM v_a,-staleA-*,v_b,-staleB-*}, the indexPattern for
 * {@code v_a} is {@code "v_a,-staleA-*,-staleB-*"} (both later exclusions apply) and for
 * {@code v_b} is {@code "v_b,-staleB-*"} (only the trailing exclusion applies — {@code -staleA-*}
 * comes before {@code v_b} in the pattern list and does not affect it).
 */
public record ResolvedView(String name, String query, String indexPattern) {
    public ResolvedView(String name, String query) {
        this(name, query, name);
    }
}
