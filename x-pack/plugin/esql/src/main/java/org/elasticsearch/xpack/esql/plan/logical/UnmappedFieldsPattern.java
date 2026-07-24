/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.regex.Regex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;

/**
 * Describes which additional (not already in {@link EsRelation}) source fields a
 * plan node would propagate to its output.
 *
 * <p>An additional source field name {@code f} is "kept" if:
 * <ol>
 *   <li>it matches <em>all</em> include patterns (AND semantics), AND</li>
 *   <li>it matches <em>no</em> exclude pattern.</li>
 * </ol>
 * AND semantics mirror what the query actually does: {@code KEEP foo* | KEEP foobar*} discards any
 * field that does not satisfy both patterns, so the loader must apply the same intersection.
 * Patterns use Elasticsearch wildcard syntax where {@code *} matches any sequence of
 * characters.
 *
 * <p>The two sentinels are {@link #ALL} and {@link #NONE}.
 * {@link #ALL} represents the case where no projection or shadowing has been applied
 * and every additional source field would pass through.
 * {@link #NONE} means no additional source field survives (e.g., when the upstream
 * plan is not an {@link EsRelation}).
 *
 * <p>The pattern for a plan is computed by the analyzer's {@code DetermineUnmappedFieldsToKeep} rule.
 */
public final class UnmappedFieldsPattern implements NamedWriteable {
    private static final List<String> INCLUDES_ALL = List.of("*");

    /** Keep every additional source field (no filtering applied). */
    public static final UnmappedFieldsPattern ALL = new UnmappedFieldsPattern(INCLUDES_ALL, List.of());

    /** Keep no additional source fields. */
    public static final UnmappedFieldsPattern NONE = new UnmappedFieldsPattern(List.of(), List.of());

    private final List<String> includes;
    private final List<String> excludes;

    public static UnmappedFieldsPattern excludes(List<String> excludes) {
        return excludes.isEmpty() ? ALL : new UnmappedFieldsPattern(INCLUDES_ALL, excludes);
    }

    public static UnmappedFieldsPattern includes(List<String> includes) {
        return includes.isEmpty() ? NONE : new UnmappedFieldsPattern(includes, List.of());
    }

    private UnmappedFieldsPattern(List<String> includes, List<String> excludes) {
        this.includes = includes;
        this.excludes = excludes;
    }

    /** Returns the intersection pattern, i.e., a field would match iff it matches both this and the other pattern. */
    public UnmappedFieldsPattern intersect(UnmappedFieldsPattern other) {
        return isNone() || other.isNone()
            ? NONE
            : new UnmappedFieldsPattern(effectiveIncludes(other), combineDeduping(excludes, other.excludes));
    }

    private static List<String> combineDeduping(List<String> l1, List<String> l2) {
        LinkedHashSet<String> merged = new LinkedHashSet<>(l1.size() + l2.size());
        merged.addAll(l1);
        merged.addAll(l2);
        return new ArrayList<>(merged);
    }

    private List<String> effectiveIncludes(UnmappedFieldsPattern other) {
        if (includes.equals(INCLUDES_ALL)) {
            return other.includes;
        }
        if (other.includes.equals(INCLUDES_ALL)) {
            return includes;
        }
        return combineDeduping(includes, other.includes);
    }

    /**
     * Whether a candidate additional source field {@code name} survives this pattern: the includes must
     * impose a restriction (be non-empty), {@code name} must match none of the excludes, and it must match
     * every include.
     */
    public boolean matches(String name) {
        return isNone() == false
            && excludes.stream().noneMatch(exclude -> Regex.simpleMatch(exclude, name))
            && includes.stream().allMatch(include -> Regex.simpleMatch(include, name));
    }

    /**
     * Returns a new pattern with {@code names} appended to the excludes list, deduplicating.
     */
    public UnmappedFieldsPattern withAdditionalExcludes(List<String> names) {
        if (names.isEmpty() || this.isNone()) {
            return this;
        }
        LinkedHashSet<String> merged = new LinkedHashSet<>(excludes.size() + names.size());
        merged.addAll(excludes);
        merged.addAll(names);
        return new UnmappedFieldsPattern(includes, new ArrayList<>(merged));
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (UnmappedFieldsPattern) obj;
        return Objects.equals(this.includes, that.includes) && Objects.equals(this.excludes, that.excludes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(includes, excludes);
    }

    @Override
    public String toString() {
        return "UnmappedFieldsPattern[" + "includes=" + includes + ", " + "excludes=" + excludes + ']';
    }

    public boolean isNone() {
        return includes.isEmpty();
    }

    @Override
    public String getWriteableName() {
        return "UnmappedFieldsPattern";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringCollection(includes);
        out.writeStringCollection(excludes);
    }
}
