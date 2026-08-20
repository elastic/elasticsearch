/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.test.AbstractNamedWriteableTestCase;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.expression.UnresolvedNamePattern;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.List;

import static org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsPattern.keepOrdered;

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
        assertTrue(exact.objectSubfieldsCouldMatch("unmapped"));
        assertTrue(exact.objectSubfieldsCouldMatch("id"));

        // A prefix wildcard ending in * covers the whole subtree, so it prunes the object at the data node.
        UnmappedFieldsPattern prefix = UnmappedFieldsPattern.excludes(List.of("unmapped*"));
        assertFalse(prefix.objectSubfieldsCouldMatch("unmapped"));
        assertTrue(prefix.objectSubfieldsCouldMatch("other"));

        // A fixed-suffix or interior wildcard matches the parent name but not its deeper leaves, so it must NOT prune the object here
        // (regression guard: this used to prune and silently lose synthetic-source leaves that a stored source would have kept).
        assertTrue(UnmappedFieldsPattern.excludes(List.of("*ped")).objectSubfieldsCouldMatch("unmapped"));
        assertTrue(UnmappedFieldsPattern.excludes(List.of("un*ped")).objectSubfieldsCouldMatch("unmapped"));

        // A nested-wildcard drop targets a descendant subtree, not the parent, so the parent object still ships.
        UnmappedFieldsPattern nested = UnmappedFieldsPattern.excludes(List.of("unmapped.deep*"));
        assertTrue(nested.objectSubfieldsCouldMatch("unmapped"));

        assertFalse(UnmappedFieldsPattern.NONE.objectSubfieldsCouldMatch("unmapped"));
        assertTrue(UnmappedFieldsPattern.ALL.objectSubfieldsCouldMatch("unmapped"));
    }

    public void testObjectPushShipsOnlyObjectsAnIncludeGroupCanReach() {
        // KEEP network keeps the object "network" (the reference reads null, but a descendant leaf could survive under a wildcard leg);
        // an unrelated object no include group can reach is pruned at the data node instead of shipped for the coordinator to discard.
        UnmappedFieldsPattern keepExact = UnmappedFieldsPattern.includes(List.of("network")).withAdditionalExcludes(List.of("network"));
        assertTrue(keepExact.objectSubfieldsCouldMatch("network"));
        assertFalse(keepExact.objectSubfieldsCouldMatch("unrelated"));

        UnmappedFieldsPattern keepWildcard = UnmappedFieldsPattern.includes(List.of("keep*"));
        assertTrue(keepWildcard.objectSubfieldsCouldMatch("keep_me"));
        assertFalse(keepWildcard.objectSubfieldsCouldMatch("other"));

        // A dotted include reaches the object whose subtree it targets, and nothing else.
        UnmappedFieldsPattern dotted = UnmappedFieldsPattern.includes(List.of("network.eth0.*"));
        assertTrue(dotted.objectSubfieldsCouldMatch("network"));
        assertFalse(dotted.objectSubfieldsCouldMatch("system"));

        // Every include group must independently be able to reach the object (chained KEEPs intersect); one that cannot prunes it.
        UnmappedFieldsPattern intersect = UnmappedFieldsPattern.includes(List.of("network.*"))
            .intersect(UnmappedFieldsPattern.includes(List.of("system.*")));
        assertFalse(intersect.objectSubfieldsCouldMatch("network"));
        assertFalse(intersect.objectSubfieldsCouldMatch("system"));
    }

    public void testObjectPushNeverUnderShipsAReachableDescendant() {
        assertTrue(UnmappedFieldsPattern.includes(List.of("a.*.leaf")).objectSubfieldsCouldMatch("a.b"));
        assertTrue(UnmappedFieldsPattern.includes(List.of("*.leaf")).objectSubfieldsCouldMatch("a"));
        assertTrue(UnmappedFieldsPattern.includes(List.of("a*")).objectSubfieldsCouldMatch("abc"));
        assertTrue(UnmappedFieldsPattern.includes(List.of("unmapped.deep.leaf")).objectSubfieldsCouldMatch("unmapped"));
        assertFalse(UnmappedFieldsPattern.includes(List.of("ab")).objectSubfieldsCouldMatch("a"));
        assertFalse(UnmappedFieldsPattern.includes(List.of("nx.y")).objectSubfieldsCouldMatch("n"));
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

        UnmappedFieldsPattern fixedSuffixDrop = UnmappedFieldsPattern.excludes(List.of("*ped"));
        assertFalse(fixedSuffixDrop.matches("unmapped"));
        assertTrue(fixedSuffixDrop.matches("unmapped.deep.leaf"));
    }

    public void testExactExcludeWithLiteralStarIsMatchedLiterallyNotAsWildcard() {
        // this is about a backtick-escaped KEEP term like `samples*` that refers to a column named literally "samples*"
        UnmappedFieldsPattern pattern = UnmappedFieldsPattern.ALL.withAdditionalExcludes(List.of("samples*"));
        assertFalse(pattern.matches("samples*"));
        assertTrue(pattern.matches("samples.nested"));
        assertTrue(pattern.objectSubfieldsCouldMatch("samples"));
    }

    public void testKeepOrderedMirrorsKeepResolverPriorities() {
        List<String> cols = List.of("foo", "bar");
        assertEquals(List.of("bar", "foo"), keepOrdered(cols, List.of(pattern("*"), exact("foo"))));
        assertEquals(List.of("foo", "bar"), keepOrdered(cols, List.of(exact("foo"), pattern("*"))));
        assertEquals(List.of("bar", "foo"), keepOrdered(cols, List.of(pattern("bar*"), exact("foo"), pattern("*"))));
        assertEquals(List.of("bar", "foo"), keepOrdered(cols, List.of(pattern("foo*"), exact("bar"), pattern("fo*"))));
    }

    public void testKeepOrderedInterleavesRealColumnsAndExpandedLeaves() {
        List<String> childOutput = List.of("id", "unmapped", "unmapped.bar", "unmapped.deep.leaf", "unmapped.foo");
        assertEquals(
            List.of("id", "unmapped.bar", "unmapped.deep.leaf", "unmapped.foo", "unmapped"),
            keepOrdered(childOutput, List.of(pattern("*"), pattern("unmapped.*"), exact("unmapped")))
        );
        assertEquals(
            List.of("unmapped.bar", "unmapped.deep.leaf", "unmapped.foo", "unmapped", "id"),
            keepOrdered(childOutput, List.of(pattern("unmapped.*"), exact("unmapped"), pattern("*")))
        );
        assertEquals(
            List.of("unmapped.bar", "unmapped.deep.leaf", "unmapped.foo", "id", "unmapped"),
            keepOrdered(childOutput, List.of(pattern("unmapped.*"), exact("id"), exact("unmapped")))
        );
    }

    public void testKeepOrderedAppendsUnmatchedColumnsLast() {
        assertEquals(
            List.of("unmapped.foo", "x"),
            keepOrdered(List.of("x", "unmapped.foo"), List.of(pattern("unmapped.*")))
        );
    }

    public void testKeepOrderedTreatsQuotedNameContainingStarAsExplicit() {
        List<String> childOutput = List.of("foobar", "foo*", "foo.leaf");
        assertEquals(List.of("foo*", "foobar", "foo.leaf"), keepOrdered(childOutput, List.of(exact("foo*"), pattern("*"))));
        assertEquals(List.of("foobar", "foo*", "foo.leaf"), keepOrdered(childOutput, List.of(pattern("foo*"), pattern("*"))));
    }

    public void testForKeepExactNamesDontReachIncludeGroups() {
        var exactOnly = UnmappedFieldsPattern.forKeep(List.of(new UnresolvedAttribute(Source.EMPTY, "foo")));
        assertTrue(exactOnly.isNone());
        assertFalse(exactOnly.objectSubfieldsCouldMatch("foo"));

        var mixed = UnmappedFieldsPattern.forKeep(
            List.of(new UnresolvedAttribute(Source.EMPTY, "foo"), new UnresolvedNamePattern(Source.EMPTY, null, "bar*", "bar*"))
        );
        assertFalse(mixed.objectSubfieldsCouldMatch("foo"));
        assertTrue(mixed.objectSubfieldsCouldMatch("bar"));
    }

    private static UnmappedFieldsPattern.KeepTerm pattern(String name) {
        return new UnmappedFieldsPattern.KeepTerm(name, true);
    }

    private static UnmappedFieldsPattern.KeepTerm exact(String name) {
        return new UnmappedFieldsPattern.KeepTerm(name, false);
    }
}
