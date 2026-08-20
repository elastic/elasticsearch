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
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedStar;
import org.elasticsearch.xpack.esql.core.expression.UnsupportedAttribute;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.expression.UnresolvedNamePattern;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Describes which additional (not already in {@link EsRelation}) source fields a
 * plan node would propagate to its output.
 *
 * <p>Includes are stored as a conjunction of OR groups. An additional source field name {@code f}
 * is "kept" if:
 * <ol>
 *   <li>for <em>every</em> include group, {@code f} matches <em>at least one</em> pattern in that
 *       group (OR within a group, AND across groups), and</li>
 *   <li>{@code f} matches no glob exclude (a {@code DROP} wildcard) and equals no exact exclude (an already-output column name).</li>
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
    public static final UnmappedFieldsPattern ALL = new UnmappedFieldsPattern(INCLUDES_ALL, List.of(), List.of());

    /** Keep no additional source fields. */
    public static final UnmappedFieldsPattern NONE = new UnmappedFieldsPattern(List.of(), List.of(), List.of());

    private final List<List<String>> includeGroups;

    /** Glob patterns (DROP wildcards) */
    private final List<String> excludes;

    /** Literal names of columns the plan already outputs, matched exactly */
    private final Set<String> exactExcludes;

    public static UnmappedFieldsPattern excludes(List<String> excludes) {
        return excludes.isEmpty() ? ALL : new UnmappedFieldsPattern(INCLUDES_ALL, excludes, List.of());
    }

    public static UnmappedFieldsPattern includes(List<String> includes) {
        return includes.isEmpty() ? NONE : new UnmappedFieldsPattern(List.of(includes), List.of(), List.of());
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
            // mirrors Analyzer.keepResolver: only a wildcard contributes an include group; an explicit name (resolved or not) is
            // demand-loaded instead, so it contributes nothing to the expansion
            switch (proj) {
                case UnresolvedStar ignored -> {
                    return ALL;
                }
                case UnresolvedNamePattern unp -> includes.add(unp.pattern());
                case UnsupportedAttribute ignored -> {
                }
                case UnresolvedAttribute ignored -> {
                }
                case NamedExpression ne when ne.resolved() -> {
                }
                default -> throw new IllegalStateException("Unsupported KEEP projection [" + proj + "]");
            }
        }
        return includes(includes);
    }

    /**
     * One {@code KEEP} projection term in written order, tagged {@code pattern} when it is a wildcard (matched with
     * {@link Regex#simpleMatch}) rather than an explicit name (matched exactly)
     */
    public record KeepTerm(String name, boolean pattern) implements Writeable {
        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeBoolean(pattern);
        }

        public static KeepTerm readFrom(StreamInput in) throws IOException {
            return new KeepTerm(in.readString(), in.readBoolean());
        }
    }

    /**
     * The ordered projection terms of a {@code KEEP} command, tagged wildcard-vs-explicit by kind (see {@link KeepTerm}). Unlike
     * {@link #forKeep}'s unordered membership predicate, this preserves written order so {@link #keepOrdered} can replay {@code KEEP}'s
     * column ordering over the expanded leaves.
     */
    public static List<KeepTerm> orderTerms(List<? extends NamedExpression> projections) {
        List<KeepTerm> terms = new ArrayList<>(projections.size());
        for (NamedExpression proj : projections) {
            switch (proj) {
                case UnresolvedStar ignored -> terms.add(new KeepTerm("*", true));
                case UnresolvedNamePattern unp -> terms.add(new KeepTerm(unp.pattern(), true));
                case UnsupportedAttribute ua -> terms.add(new KeepTerm(ua.name(), false));
                case UnresolvedAttribute ua -> terms.add(new KeepTerm(ua.name(), false));
                case NamedExpression ne when ne.resolved() -> terms.add(new KeepTerm(ne.name(), false));
                default -> throw new IllegalStateException("Unsupported KEEP projection [" + proj + "]");
            }
        }
        return terms;
    }

    /**
     * Replays {@code KEEP}'s column-ordering contract over {@code childOutput} — the real columns followed by the discovered unmapped
     * leaves (alphabetical) — using {@code keepTerms} from {@link #orderTerms}.
     */
    public static List<String> keepOrdered(List<String> childOutput, List<KeepTerm> keepTerms) {
        LinkedHashMap<String, Integer> priorities = new LinkedHashMap<>();
        for (KeepTerm term : keepTerms) {
            boolean explicit = term.pattern() == false;
            // Priorities match keepResolver's: an explicit name (1) outranks any wildcard, and a non-bare wildcard (3) outranks bare * (4).
            int priority = explicit ? 1 : (term.name().equals("*") ? 4 : 3);
            for (String name : childOutput) {
                boolean matched = explicit ? name.equals(term.name()) : Regex.simpleMatch(term.name(), name);
                if (matched) {
                    Integer previous = priorities.get(name);
                    if (previous == null || previous >= priority) {
                        priorities.remove(name);
                        priorities.put(name, priority);
                    }
                }
            }
        }
        List<String> ordered = new ArrayList<>(priorities.keySet());
        for (String name : childOutput) {
            if (priorities.containsKey(name) == false) {
                ordered.add(name);
            }
        }
        return ordered;
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

    private UnmappedFieldsPattern(List<List<String>> includeGroups, List<String> excludes, Collection<String> exactExcludes) {
        this.includeGroups = includeGroups.stream().map(List::copyOf).toList();
        this.excludes = List.copyOf(excludes);
        this.exactExcludes = Set.copyOf(new LinkedHashSet<>(exactExcludes));
    }

    /**
     * Returns the intersection pattern, i.e., a field would match iff it matches both this and the other pattern.
     * Excludes (both glob and exact) from both patterns are merged.
     */
    public UnmappedFieldsPattern intersect(UnmappedFieldsPattern other) {
        return isNone() || other.isNone()
            ? NONE
            : new UnmappedFieldsPattern(
                effectiveIncludeGroups(other),
                combineDeduping(excludes, other.excludes),
                combineDeduping(exactExcludes, other.exactExcludes)
            );
    }

    private static List<String> combineDeduping(Collection<String> l1, Collection<String> l2) {
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
            && exactExcludes.contains(name) == false
            && excludes.stream().noneMatch(exclude -> Regex.simpleMatch(exclude, name))
            && includeGroups.stream().allMatch(group -> group.stream().anyMatch(include -> Regex.simpleMatch(include, name)));
    }

    /**
     * Whether a top-level {@code _source} object or array key should ship from the data node so the coordinator can flatten it into dotted
     * leaf columns and keep the surviving ones. It owns no column, so it ships if every include group could reach the key or a descendant
     * ({@link #groupCouldMatchDescendant}); the per-group over-approximation only ever over-ships, never dropping a leaf that survives.
     */
    public boolean matchesObjectPush(String name) {
        if (isNone() || anySubtreeCoveringExcludeMatches(name)) {
            return false;
        }
        for (List<String> group : includeGroups) {
            if (groupCouldMatchDescendant(group, name) == false) {
                return false;
            }
        }
        return true;
    }

    /**
     * Whether some pattern in {@code group} could match object {@code name} or a {@code name.*} descendant
     */
    private static boolean groupCouldMatchDescendant(List<String> group, String name) {
        String dotted = name + ".";
        for (String pattern : group) {
            if (Regex.simpleMatch(pattern, name) || pattern.startsWith(dotted)) {
                return true;
            }
            int wildcard = pattern.indexOf('*');
            if (wildcard >= 0 && dotted.startsWith(pattern.substring(0, wildcard))) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether any exclude covers the <em>entire</em> subtree of {@code name}: it ends in {@code *} (so its trailing wildcard absorbs an
     * arbitrarily long {@code .child} suffix) and matches {@code name} itself. A fixed-suffix wildcard like {@code *d} matches the parent
     * but not its leaves, so it is not subtree-covering and is left to the coordinator's per-leaf {@link #matches}.
     */
    private boolean anySubtreeCoveringExcludeMatches(String name) {
        return excludes.stream().anyMatch(exclude -> exclude.endsWith("*") && Regex.simpleMatch(exclude, name));
    }

    /**
     * Returns a new pattern with {@code names} appended to the exact-name excludes (matched literally, not as globs), deduplicating.
     */
    public UnmappedFieldsPattern withAdditionalExcludes(List<String> names) {
        if (names.isEmpty() || this.isNone()) {
            return this;
        }
        LinkedHashSet<String> merged = new LinkedHashSet<>(exactExcludes.size() + names.size());
        merged.addAll(exactExcludes);
        merged.addAll(names);
        return new UnmappedFieldsPattern(includeGroups, excludes, merged);
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
        return Objects.equals(this.includeGroups, that.includeGroups)
            && Objects.equals(this.excludes, that.excludes)
            && Objects.equals(this.exactExcludes, that.exactExcludes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(includeGroups, excludes, exactExcludes);
    }

    @Override
    public String toString() {
        return "UnmappedFieldsPattern[includeGroups=" + includeGroups + ", excludes=" + excludes + ", exactExcludes=" + exactExcludes + ']';
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
        out.writeStringCollection(exactExcludes);
    }

    public static UnmappedFieldsPattern readFrom(StreamInput in) throws IOException {
        return new UnmappedFieldsPattern(
            in.readCollectionAsList(StreamInput::readStringCollectionAsList),
            in.readStringCollectionAsList(),
            in.readStringCollectionAsList()
        );
    }
}
