/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * Describes which additional (not already in {@link EsRelation}) source fields a
 * plan node would propagate to its output.
 *
 * <p>An additional source field name {@code f} is "kept" if:
 * <ol>
 *   <li>it matches at least one <em>include</em> pattern, AND</li>
 *   <li>it matches <em>no</em> exclude pattern.</li>
 * </ol>
 * Patterns use Elasticsearch wildcard syntax where {@code *} matches any sequence of
 * characters.
 *
 * <p>The two sentinels are {@link #ALL} and {@link #NONE}.
 * {@link #ALL} represents the case where no projection or shadowing has been applied
 * and every additional source field would pass through.
 * {@link #NONE} means no additional source field survives (e.g., when the upstream
 * plan is not an {@link EsRelation}).
 *
 * @see LogicalPlan#unmappedFieldsToKeep()
 */
public record UnmappedFieldsPattern(List<String> includes, List<String> excludes) {

    /** Keep every additional source field (no filtering applied). */
    public static final UnmappedFieldsPattern ALL = new UnmappedFieldsPattern(List.of("*"), List.of());

    /** Keep no additional source fields. */
    public static final UnmappedFieldsPattern NONE = new UnmappedFieldsPattern(List.of(), List.of());

    public UnmappedFieldsPattern {
        includes = List.copyOf(includes);
        excludes = List.copyOf(excludes);
    }

    /** Returns a new pattern with {@code names} appended to the excludes list, deduplicating. */
    public UnmappedFieldsPattern withAdditionalExcludes(List<String> names) {
        if (names.isEmpty()) return this;
        LinkedHashSet<String> merged = new LinkedHashSet<>(excludes.size() + names.size());
        merged.addAll(excludes);
        merged.addAll(names);
        return new UnmappedFieldsPattern(includes, new ArrayList<>(merged));
    }
}
