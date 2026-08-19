/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.test.AbstractNamedWriteableTestCase;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnsupportedAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.UnsupportedEsField;

import java.util.List;

public class UnmappedFieldsPatternTests extends AbstractNamedWriteableTestCase<UnmappedFieldsPattern> {

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(List.of(UnmappedFieldsPattern.ENTRY));
    }

    @Override
    protected Class<UnmappedFieldsPattern> categoryClass() {
        return UnmappedFieldsPattern.class;
    }

    @Override
    protected UnmappedFieldsPattern createTestInstance() {
        return switch (between(0, 3)) {
            case 0 -> UnmappedFieldsPattern.ALL;
            case 1 -> UnmappedFieldsPattern.NONE;
            case 2 -> UnmappedFieldsPattern.includes(List.of("first*", "given*"))
                .intersect(UnmappedFieldsPattern.includes(List.of("last*", "family*")))
                .withAdditionalExcludes(List.of("secret*", "emp_no"));
            case 3 -> UnmappedFieldsPattern.excludes(List.of(randomAlphaOfLength(4) + "*"));
            default -> throw new AssertionError("unreachable");
        };
    }

    @Override
    protected UnmappedFieldsPattern mutateInstance(UnmappedFieldsPattern instance) {
        if (instance.isNone()) {
            return UnmappedFieldsPattern.ALL;
        }
        if (instance.equals(UnmappedFieldsPattern.ALL)) {
            return UnmappedFieldsPattern.NONE;
        }
        return randomBoolean()
            ? instance.intersect(UnmappedFieldsPattern.includes(List.of("mutation_" + randomAlphaOfLength(4) + "*")))
            : instance.withAdditionalExcludes(List.of("mutation_" + randomAlphaOfLength(4)));
    }

    public void testObjectPushPrunesOnlyOnSubtreeCoveringExcludes() {
        // Exact excludes - what DetermineUnmappedFieldsToKeep adds for a referenced/dropped/mapped column - never prune an object.
        UnmappedFieldsPattern exact = UnmappedFieldsPattern.excludes(List.of("unmapped", "id"));
        assertTrue(exact.matchesObjectPush("unmapped"));
        assertTrue(exact.matchesObjectPush("id"));

        // A prefix wildcard ending in * covers the whole subtree, so it prunes the object at the data node.
        UnmappedFieldsPattern prefix = UnmappedFieldsPattern.excludes(List.of("unmapped*"));
        assertFalse(prefix.matchesObjectPush("unmapped"));
        assertTrue(prefix.matchesObjectPush("other"));

        // A fixed-suffix or interior wildcard matches the parent name but not its deeper leaves, so it must NOT prune the object here
        // (regression guard: this used to prune and silently lose synthetic-source leaves that a stored source would have kept).
        assertTrue(UnmappedFieldsPattern.excludes(List.of("*ped")).matchesObjectPush("unmapped"));
        assertTrue(UnmappedFieldsPattern.excludes(List.of("un*ped")).matchesObjectPush("unmapped"));

        // A nested-wildcard drop targets a descendant subtree, not the parent, so the parent object still ships.
        UnmappedFieldsPattern nested = UnmappedFieldsPattern.excludes(List.of("unmapped.deep*"));
        assertTrue(nested.matchesObjectPush("unmapped"));

        assertFalse(UnmappedFieldsPattern.NONE.matchesObjectPush("unmapped"));
        assertTrue(UnmappedFieldsPattern.ALL.matchesObjectPush("unmapped"));
    }

    public void testObjectPushShipsOnlyObjectsAnIncludeGroupCanReach() {
        // KEEP network keeps the object "network" (the reference reads null, but a descendant leaf could survive under a wildcard leg);
        // an unrelated object no include group can reach is pruned at the data node instead of shipped for the coordinator to discard.
        UnmappedFieldsPattern keepExact = UnmappedFieldsPattern.includes(List.of("network")).withAdditionalExcludes(List.of("network"));
        assertTrue(keepExact.matchesObjectPush("network"));
        assertFalse(keepExact.matchesObjectPush("unrelated"));

        UnmappedFieldsPattern keepWildcard = UnmappedFieldsPattern.includes(List.of("keep*"));
        assertTrue(keepWildcard.matchesObjectPush("keep_me"));
        assertFalse(keepWildcard.matchesObjectPush("other"));

        // A dotted include reaches the object whose subtree it targets, and nothing else.
        UnmappedFieldsPattern dotted = UnmappedFieldsPattern.includes(List.of("network.eth0.*"));
        assertTrue(dotted.matchesObjectPush("network"));
        assertFalse(dotted.matchesObjectPush("system"));

        // Every include group must independently be able to reach the object (chained KEEPs intersect); one that cannot prunes it.
        UnmappedFieldsPattern intersect = UnmappedFieldsPattern.includes(List.of("network.*"))
            .intersect(UnmappedFieldsPattern.includes(List.of("system.*")));
        assertFalse(intersect.matchesObjectPush("network"));
        assertFalse(intersect.matchesObjectPush("system"));
    }

    /**
     * {@link UnmappedFieldsPattern#matchesObjectPush} must never under-ship: whenever a pattern can match some descendant leaf of the
     * object, the object is shipped so the coordinator sees that leaf - even for a mid-pattern wildcard over a literal dotted object key.
     */
    public void testObjectPushNeverUnderShipsAReachableDescendant() {
        assertTrue(UnmappedFieldsPattern.includes(List.of("a.*.leaf")).matchesObjectPush("a.b"));
        assertTrue(UnmappedFieldsPattern.includes(List.of("*.leaf")).matchesObjectPush("a"));
        assertTrue(UnmappedFieldsPattern.includes(List.of("a*")).matchesObjectPush("abc"));
        assertTrue(UnmappedFieldsPattern.includes(List.of("unmapped.deep.leaf")).matchesObjectPush("unmapped"));
        // A literal that is neither the object nor a descendant of it cannot produce a surviving leaf, so the object is pruned.
        assertFalse(UnmappedFieldsPattern.includes(List.of("ab")).matchesObjectPush("a"));
        assertFalse(UnmappedFieldsPattern.includes(List.of("nx.y")).matchesObjectPush("n"));
    }

    public void testMatchesGovernsDottedLeavesForSourceParity() {
        UnmappedFieldsPattern exactExclude = UnmappedFieldsPattern.excludes(List.of("unmapped"));
        assertFalse(exactExclude.matches("unmapped"));
        assertTrue(exactExclude.matches("unmapped.deep.leaf"));

        UnmappedFieldsPattern childWildcardKeep = UnmappedFieldsPattern.includes(List.of("unmapped.*"));
        assertTrue(childWildcardKeep.matches("unmapped.deep.leaf"));
        assertTrue(childWildcardKeep.matches("unmapped.foo"));
        assertFalse(childWildcardKeep.matches("unmapped"));
        assertFalse(childWildcardKeep.matches("other.leaf"));

        UnmappedFieldsPattern nestedWildcardDrop = UnmappedFieldsPattern.excludes(List.of("unmapped.deep*"));
        assertFalse(nestedWildcardDrop.matches("unmapped.deep.leaf"));
        assertTrue(nestedWildcardDrop.matches("unmapped.foo"));

        UnmappedFieldsPattern exactInclude = UnmappedFieldsPattern.includes(List.of("network"));
        assertTrue(exactInclude.matches("network"));
        assertFalse(exactInclude.matches("network.bytes_in"));

        // A fixed-suffix wildcard drops the parent name but keeps deeper leaves that do not share the suffix - exactly why the data-node
        // object push must not prune the object on it, since the coordinator still keeps unmapped.deep.leaf here.
        UnmappedFieldsPattern fixedSuffixDrop = UnmappedFieldsPattern.excludes(List.of("*ped"));
        assertFalse(fixedSuffixDrop.matches("unmapped"));
        assertTrue(fixedSuffixDrop.matches("unmapped.deep.leaf"));
    }

    public void testExactExcludeWithLiteralStarIsMatchedLiterallyNotAsWildcard() {
        // A backtick-escaped KEEP term like `samples*` demand-loads a column named literally "samples*", added here as an exact exclude. It
        // must exclude only that column, not glob-match sibling leaves like samples.nested (data-node object push and coordinator filter).
        UnmappedFieldsPattern pattern = UnmappedFieldsPattern.ALL.withAdditionalExcludes(List.of("samples*"));
        assertFalse(pattern.matches("samples*"));
        assertTrue(pattern.matches("samples.nested"));
        assertTrue(pattern.matchesObjectPush("samples"));
    }

    /**
     * {@link UnmappedFieldsPattern#keepOrdered} reproduces {@code Analyzer.keepResolver}'s ordering: priority is explicit name &gt;
     * non-bare wildcard &gt; bare {@code *}, and a later term of equal-or-higher priority moves a column to the end. These are the worked
     * examples from that method's javadoc.
     */
    public void testKeepOrderedMirrorsKeepResolverPriorities() {
        List<String> cols = List.of("foo", "bar");
        assertEquals(List.of("bar", "foo"), UnmappedFieldsPattern.keepOrdered(cols, List.of(pat("*"), lit("foo"))));
        assertEquals(List.of("foo", "bar"), UnmappedFieldsPattern.keepOrdered(cols, List.of(lit("foo"), pat("*"))));
        assertEquals(List.of("bar", "foo"), UnmappedFieldsPattern.keepOrdered(cols, List.of(pat("bar*"), lit("foo"), pat("*"))));
        assertEquals(List.of("bar", "foo"), UnmappedFieldsPattern.keepOrdered(cols, List.of(pat("foo*"), lit("bar"), pat("fo*"))));
    }

    /**
     * The LOAD_ALL case this ordering exists for: the real columns and runtime-discovered leaves (alphabetical in {@code childOutput})
     * are reordered to honor KEEP's left-to-right contract - including moving a real column ({@code id}) after the expanded leaves.
     */
    public void testKeepOrderedInterleavesRealColumnsAndExpandedLeaves() {
        List<String> childOutput = List.of("id", "unmapped", "unmapped.bar", "unmapped.deep.leaf", "unmapped.foo");
        assertEquals(
            List.of("id", "unmapped.bar", "unmapped.deep.leaf", "unmapped.foo", "unmapped"),
            UnmappedFieldsPattern.keepOrdered(childOutput, List.of(pat("*"), pat("unmapped.*"), lit("unmapped")))
        );
        assertEquals(
            List.of("unmapped.bar", "unmapped.deep.leaf", "unmapped.foo", "unmapped", "id"),
            UnmappedFieldsPattern.keepOrdered(childOutput, List.of(pat("unmapped.*"), lit("unmapped"), pat("*")))
        );
        // An explicit real column pinned between two leaf-producing terms: id lands after unmapped.* leaves but before the scalar unmapped.
        assertEquals(
            List.of("unmapped.bar", "unmapped.deep.leaf", "unmapped.foo", "id", "unmapped"),
            UnmappedFieldsPattern.keepOrdered(childOutput, List.of(pat("unmapped.*"), lit("id"), lit("unmapped")))
        );
    }

    /**
     * A {@code childOutput} name that no term matches - e.g. a column an {@code EVAL} added above the governing {@code KEEP} - keeps its
     * natural trailing position, so reordering never drops a column.
     */
    public void testKeepOrderedAppendsUnmatchedColumnsLast() {
        assertEquals(
            List.of("unmapped.foo", "x"),
            UnmappedFieldsPattern.keepOrdered(List.of("x", "unmapped.foo"), List.of(pat("unmapped.*")))
        );
    }

    /**
     * A quoted name that contains {@code *} is an explicit projection (its {@link UnmappedFieldsPattern.KeepTerm} is not a pattern), so
     * it matches only the column named exactly {@code foo*}; the pattern {@code foo*} instead globs over every {@code foo}-prefixed
     * column. Re-deriving the tag from the string would conflate the two - the misclassification the type-carried tag prevents.
     */
    public void testKeepOrderedTreatsQuotedNameContainingStarAsExplicit() {
        List<String> childOutput = List.of("foobar", "foo*", "foo.leaf");
        assertEquals(List.of("foo*", "foobar", "foo.leaf"), UnmappedFieldsPattern.keepOrdered(childOutput, List.of(lit("foo*"), pat("*"))));
        assertEquals(List.of("foobar", "foo*", "foo.leaf"), UnmappedFieldsPattern.keepOrdered(childOutput, List.of(pat("foo*"), pat("*"))));
    }

    /**
     * {@link UnmappedFieldsPattern#orderTerms} and {@link UnmappedFieldsPattern#forKeep} accept every projection kind
     * {@code Analyzer.keepResolver} does - including an {@link UnsupportedAttribute} and an already-resolved attribute - so computing
     * cosmetic ordering never throws. All three are explicit references, tagged non-pattern and contributing no include group.
     */
    public void testOrderTermsAndForKeepAcceptEveryKeepResolverKind() {
        List<NamedExpression> projections = List.of(
            new UnresolvedAttribute(Source.EMPTY, "foo*"),
            new UnsupportedAttribute(Source.EMPTY, "unsup", new UnsupportedEsField("unsup", List.of("keyword", "long"))),
            new ReferenceAttribute(Source.EMPTY, "resolved", DataType.KEYWORD)
        );
        assertEquals(List.of(lit("foo*"), lit("unsup"), lit("resolved")), UnmappedFieldsPattern.orderTerms(projections));
        assertTrue(UnmappedFieldsPattern.forKeep(projections).isNone());
    }

    private static UnmappedFieldsPattern.KeepTerm pat(String name) {
        return new UnmappedFieldsPattern.KeepTerm(name, true);
    }

    private static UnmappedFieldsPattern.KeepTerm lit(String name) {
        return new UnmappedFieldsPattern.KeepTerm(name, false);
    }
}
