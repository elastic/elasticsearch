/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedStar;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.expression.UnresolvedNamePattern;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;

/**
 * Describes which additional (not already in {@link EsRelation}) source fields a
 * plan node would propagate to its output.
 *
 * <p>Includes are stored as a conjunction of OR groups. An additional source field name {@code f}
 * is "kept" if:
 * <ol>
 *   <li>for <em>every</em> include group, {@code f} matches <em>at least one</em> pattern in that
 *       group (OR within a group, AND across groups), and</li>
 *   <li>{@code f} matches <em>no</em> exclude pattern.</li>
 * </ol>
 * This mirrors KEEP semantics: terms listed in one {@code KEEP} command are alternatives, while
 * chained {@code KEEP} commands intersect their selections. For example,
 * {@code KEEP first*, salary_bonus* | KEEP first_name*} keeps {@code first_name_suffix} (matches
 * {@code first*} in the first KEEP and {@code first_name*} in the second) but not {@code first_grade}
 * (matches only the first group).
 *
 * <p>The two sentinels are {@link #ALL} and {@link #NONE}.
 * {@link #ALL} represents the case where no projection or shadowing has been applied
 * and every additional source field would pass through.
 * {@link #NONE} means no additional source field survives (e.g., when the upstream
 * plan is not an {@link EsRelation}).
 */
public final class UnmappedFieldsPattern implements NamedWriteable {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        UnmappedFieldsPattern.class,
        "UnmappedFieldsPattern",
        UnmappedFieldsPattern::readFrom
    );

    private static final List<List<String>> INCLUDES_ALL = List.of(List.of("*"));

    /** Keep every additional source field (no filtering applied). */
    public static final UnmappedFieldsPattern ALL = new UnmappedFieldsPattern(INCLUDES_ALL, List.of());

    /** Keep no additional source fields. */
    public static final UnmappedFieldsPattern NONE = new UnmappedFieldsPattern(List.of(), List.of());

    private final List<List<String>> includeGroups;
    private final List<String> excludes;

    public static UnmappedFieldsPattern excludes(List<String> excludes) {
        return excludes.isEmpty() ? ALL : new UnmappedFieldsPattern(INCLUDES_ALL, excludes);
    }

    public static UnmappedFieldsPattern includes(List<String> includes) {
        return includes.isEmpty() ? NONE : new UnmappedFieldsPattern(List.of(includes), List.of());
    }

    /**
     * The pattern of a {@code KEEP} command, computed from the projection list it was written with.
     *
     * <p>Wildcard terms from this single {@code KEEP} form one OR group: a source field survives if it matches any listed
     * pattern. An all-literal {@code KEEP} therefore yields {@link #NONE}.
     */
    public static UnmappedFieldsPattern forKeep(List<? extends NamedExpression> projections) {
        List<String> includes = new ArrayList<>();
        for (NamedExpression proj : projections) {
            switch (proj) {
                case UnresolvedStar ignored -> {
                    return ALL;
                }
                case UnresolvedNamePattern unp -> includes.add(unp.pattern());
                case UnresolvedAttribute ignored -> {
                }
                default -> throw new IllegalStateException("Unsupported KEEP projection [" + proj + "]");
            }
        }
        return includes(includes);
    }

    /**
     * The pattern of a {@code DROP} command, computed from the removal list it was written with.
     *
     * <p>Only wildcard removals need to be carried: planning cannot know which unmapped source fields a wildcard
     * will match, so the pattern has to be applied during the {@code _unmapped_fields} expansion. An explicitly
     * named removal is already excluded downstream, because {@code DetermineUnmappedFieldsToKeep} excludes every
     * {@code EsRelation.output()} name — which covers both mapped columns and the fields
     * {@code ResolveUnmapped} demand-loads for explicit references.
     */
    public static UnmappedFieldsPattern forDrop(List<? extends NamedExpression> removals) {
        return excludes(
            removals.stream().filter(r -> r instanceof UnresolvedNamePattern).map(r -> ((UnresolvedNamePattern) r).pattern()).toList()
        );
    }

    private UnmappedFieldsPattern(List<List<String>> includeGroups, List<String> excludes) {
        this.includeGroups = includeGroups.stream().map(List::copyOf).toList();
        this.excludes = List.copyOf(excludes);
    }

    /**
     * Returns the intersection pattern, i.e., a field would match iff it matches both this and the other pattern.
     * Excludes from both patterns are merged.
     */
    public UnmappedFieldsPattern intersect(UnmappedFieldsPattern other) {
        return isNone() || other.isNone()
            ? NONE
            : new UnmappedFieldsPattern(effectiveIncludeGroups(other), combineDeduping(excludes, other.excludes));
    }

    private static List<String> combineDeduping(List<String> l1, List<String> l2) {
        LinkedHashSet<String> merged = new LinkedHashSet<>(l1.size() + l2.size());
        merged.addAll(l1);
        merged.addAll(l2);
        return new ArrayList<>(merged);
    }

    private List<List<String>> effectiveIncludeGroups(UnmappedFieldsPattern other) {
        if (includeGroups.equals(INCLUDES_ALL)) {
            return other.includeGroups;
        }
        if (other.includeGroups.equals(INCLUDES_ALL)) {
            return includeGroups;
        }
        return CollectionUtils.combine(includeGroups, other.includeGroups);
    }

    /**
     * Whether a candidate additional source field {@code name} survives this pattern: the include
     * groups must impose a restriction (be non-empty), {@code name} must match none of the excludes,
     * and it must match at least one pattern in every include group.
     */
    public boolean matches(String name) {
        return isNone() == false
            && excludes.stream().noneMatch(exclude -> Regex.simpleMatch(exclude, name))
            && includeGroups.stream().allMatch(group -> group.stream().anyMatch(include -> Regex.simpleMatch(include, name)));
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
        return new UnmappedFieldsPattern(includeGroups, new ArrayList<>(merged));
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }
        var that = (UnmappedFieldsPattern) obj;
        return Objects.equals(this.includeGroups, that.includeGroups) && Objects.equals(this.excludes, that.excludes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(includeGroups, excludes);
    }

    @Override
    public String toString() {
        return "UnmappedFieldsPattern[" + "includeGroups=" + includeGroups + ", " + "excludes=" + excludes + ']';
    }

    public boolean isNone() {
        return includeGroups.isEmpty();
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(includeGroups, StreamOutput::writeStringCollection);
        out.writeStringCollection(excludes);
    }

    public static UnmappedFieldsPattern readFrom(StreamInput in) throws IOException {
        return new UnmappedFieldsPattern(in.readCollectionAsList(StreamInput::readStringCollectionAsList), in.readStringCollectionAsList());
    }
}
