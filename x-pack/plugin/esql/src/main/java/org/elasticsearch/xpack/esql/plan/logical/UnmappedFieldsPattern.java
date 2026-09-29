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
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedStar;
import org.elasticsearch.xpack.esql.core.expression.UnsupportedAttribute;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.expression.UnresolvedNamePattern;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
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
 *
 * <p>Include and glob-exclude patterns use the syntax of {@link UnresolvedNamePattern#glob()}.
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

    private final List<String> globExcludes;

    // TODO: find ways of shrinking the size of this thing
    private final Set<String> exactExcludes; // this one could potentially be large and IS serialized

    // compiled once from the strings above
    private final CompiledGlob[][] compiledIncludeGroups;
    private final CompiledGlob[] compiledGlobExcludes;

    // The excludes that cover the entire subtree of a name they match: they end in an unescaped *, whose wildcard cover any .child suffix
    private final CompiledGlob[] subtreeCoveringExcludes;
    private final boolean includesAll;

    public static UnmappedFieldsPattern excludes(List<String> excludes) {
        return excludes.isEmpty() ? ALL : new UnmappedFieldsPattern(INCLUDES_ALL, excludes, List.of());
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
                case UnresolvedNamePattern unp -> includes.add(unp.glob());
                case UnsupportedAttribute ignored -> {
                }
                case UnresolvedAttribute ignored -> {
                }
                default -> throw new IllegalStateException("Unsupported KEEP projection [" + proj + "]");
            }
        }
        return includes(includes);
    }

    public static UnmappedFieldsPattern includes(List<String> includes) {
        return includes.isEmpty() ? NONE : new UnmappedFieldsPattern(List.of(includes), List.of(), List.of());
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
            removals.stream().filter(r -> r instanceof UnresolvedNamePattern).map(r -> ((UnresolvedNamePattern) r).glob()).toList()
        );
    }

    private UnmappedFieldsPattern(List<List<String>> includeGroups, List<String> globExcludes, Collection<String> exactExcludes) {
        this.includeGroups = includeGroups.stream().map(List::copyOf).toList();
        this.globExcludes = List.copyOf(globExcludes);
        this.exactExcludes = Set.copyOf(new LinkedHashSet<>(exactExcludes));
        this.compiledIncludeGroups = this.includeGroups.stream()
            .map(group -> group.stream().map(CompiledGlob::new).toArray(CompiledGlob[]::new))
            .toArray(CompiledGlob[][]::new);
        this.compiledGlobExcludes = this.globExcludes.stream().map(CompiledGlob::new).toArray(CompiledGlob[]::new);
        this.subtreeCoveringExcludes = Arrays.stream(compiledGlobExcludes)
            .filter(CompiledGlob::endsWithWildcard)
            .toArray(CompiledGlob[]::new);
        this.includesAll = this.includeGroups.equals(INCLUDES_ALL);
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
                combineDeduping(globExcludes, other.globExcludes),
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
        if (includesAll) {
            return other.includeGroups;
        }
        if (other.includesAll) {
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
        if (isNone() || exactExcludes.contains(name) || anyMatches(compiledGlobExcludes, name)) {
            return false;
        }
        if (includesAll) {
            return true;
        }
        for (CompiledGlob[] group : compiledIncludeGroups) {
            if (anyMatches(group, name) == false) {
                return false;
            }
        }
        return true;
    }

    private static boolean anyMatches(CompiledGlob[] globs, String name) {
        for (CompiledGlob glob : globs) {
            if (glob.matches(name)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether a top-level {@code _source} object or array key should ship from the data node so the coordinator can flatten it into dotted
     * leaf columns. Deliberately a superset of the per-leaf {@link #matches}: an array of scalars flattens to a leaf named exactly
     * {@code name}, so anything {@code matches} would keep has to ship from here first.
     */
    public boolean objectSubfieldsCouldMatch(String name) {
        // TODO: apply the UnmappedFieldsPattern early so that whatever is being shipped to the coordinator is bare minimum
        // (as opposed to shipping the whole _source of that specific field + its subfields)
        if (isNone() || anyMatches(subtreeCoveringExcludes, name)) {
            return false;
        }
        if (includesAll) {
            return true;
        }
        for (CompiledGlob[] group : compiledIncludeGroups) {
            if (groupCouldMatchDescendant(group, name) == false) {
                return false;
            }
        }
        return true;
    }

    /**
     * Whether some pattern in {@code group} could match object {@code name} or a {@code name.*} descendant, compared on each pattern's
     * literal head — everything before its first {@code *}, or the whole pattern when it has none.
     */
    private static boolean groupCouldMatchDescendant(CompiledGlob[] group, String name) {
        int length = name.length();
        for (CompiledGlob pattern : group) {
            String head = pattern.literalHead();
            // same as head.startsWith(name + ".") || (name + ".").startsWith(head), without allocating per key
            if (head.length() > length ? head.charAt(length) == '.' && head.startsWith(name) : name.startsWith(head)) {
                return true;
            }
        }
        return false;
    }

    /**
     * The pattern that keeps a field if {@code this} or {@code other} would keep it. Used when a {@link MergePlan} merges
     * branches that stamped different patterns: the coordinator expands extras that <em>any</em> sibling shipped, so
     * the output attribute must not inherit the first branch's restriction.
     * <p>
     * Excludes survive only when both sides would drop the name. An unrestricted include ({@code *}) on either side
     * wins. Otherwise include patterns are OR'd into one group, which is exact for typical per-branch KEEPs and an
     * over-approximation when a branch itself has chained KEEPs.
     */
    public UnmappedFieldsPattern union(UnmappedFieldsPattern other) {
        if (isNone()) {
            return other;
        }
        if (other.isNone()) {
            return this;
        }

        List<List<String>> includes = includesAll || other.includesAll
            ? INCLUDES_ALL
            : List.of(combineDeduping(flattenIncludePatterns(includeGroups), flattenIncludePatterns(other.includeGroups)));
        return new UnmappedFieldsPattern(
            includes,
            intersectDeduping(globExcludes, other.globExcludes),
            intersectDeduping(exactExcludes, other.exactExcludes)
        );
    }

    private static List<String> flattenIncludePatterns(List<List<String>> groups) {
        return groups.stream().flatMap(Collection::stream).toList();
    }

    private static List<String> intersectDeduping(Collection<String> l1, Collection<String> l2) {
        HashSet<String> l2Set = new HashSet<>(l2);
        return l1.stream().filter(l2Set::contains).distinct().toList();
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
        return new UnmappedFieldsPattern(includeGroups, globExcludes, merged);
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
            && Objects.equals(this.globExcludes, that.globExcludes)
            && Objects.equals(this.exactExcludes, that.exactExcludes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(includeGroups, globExcludes, exactExcludes);
    }

    @Override
    public String toString() {
        return "UnmappedFieldsPattern[includeGroups="
            + includeGroups
            + ", globExcludes="
            + globExcludes
            + ", exactExcludes="
            + exactExcludes
            + ']';
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
        out.writeStringCollection(globExcludes);
        out.writeStringCollection(exactExcludes);
    }

    public static UnmappedFieldsPattern readFrom(StreamInput in) throws IOException {
        return new UnmappedFieldsPattern(
            in.readCollectionAsList(StreamInput::readStringCollectionAsList),
            in.readStringCollectionAsList(),
            in.readStringCollectionAsList()
        );
    }

    /** A glob split at its unescaped *s into escape-resolved literal fragments; a single fragment means it has no wildcard. */
    private static final class CompiledGlob {
        private final String[] fragments;

        CompiledGlob(String glob) {
            List<String> parts = new ArrayList<>();
            StringBuilder fragment = new StringBuilder();
            for (int i = 0; i < glob.length(); i++) {
                char c = glob.charAt(i);
                if (c == '\\' && i + 1 < glob.length()) {
                    fragment.append(glob.charAt(++i));
                } else if (c == '*') {
                    parts.add(fragment.toString());
                    fragment.setLength(0);
                } else {
                    fragment.append(c);
                }
            }
            parts.add(fragment.toString());
            this.fragments = parts.toArray(String[]::new);
        }

        /** The escape-resolved text before the first wildcard, or the whole literal when there is none. */
        String literalHead() {
            return fragments[0];
        }

        boolean endsWithWildcard() {
            return fragments.length > 1 && fragments[fragments.length - 1].isEmpty();
        }

        boolean matches(String name) {
            String first = fragments[0];
            if (fragments.length == 1) {
                return name.equals(first);
            }
            String last = fragments[fragments.length - 1];
            if (name.length() < first.length() + last.length() || name.startsWith(first) == false || name.endsWith(last) == false) {
                return false;
            }
            int from = first.length();
            int to = name.length() - last.length();
            for (int i = 1; i < fragments.length - 1; i++) {
                String fragment = fragments[i];
                int at = name.indexOf(fragment, from);
                if (at < 0 || at + fragment.length() > to) {
                    return false;
                }
                from = at + fragment.length();
            }
            return true;
        }
    }
}
