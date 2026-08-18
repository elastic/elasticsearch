/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.test.AbstractNamedWriteableTestCase;

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

    public void testObjectPushIgnoresIncludes() {
        UnmappedFieldsPattern keepExact = UnmappedFieldsPattern.includes(List.of("network")).withAdditionalExcludes(List.of("network"));
        assertTrue(keepExact.matchesObjectPush("network"));
        assertTrue(keepExact.matchesObjectPush("unrelated"));

        UnmappedFieldsPattern keepWildcard = UnmappedFieldsPattern.includes(List.of("keep*"));
        assertTrue(keepWildcard.matchesObjectPush("keep_me"));
        assertTrue(keepWildcard.matchesObjectPush("other"));
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

    /**
     * {@link UnmappedFieldsPattern#keepOrdered} reproduces {@code Analyzer.keepResolver}'s ordering: priority is explicit name &gt;
     * non-bare wildcard &gt; bare {@code *}, and a later term of equal-or-higher priority moves a column to the end. These are the worked
     * examples from that method's javadoc.
     */
    public void testKeepOrderedMirrorsKeepResolverPriorities() {
        assertEquals(List.of("bar", "foo"), UnmappedFieldsPattern.keepOrdered(List.of("foo", "bar"), List.of("*", "foo")));
        assertEquals(List.of("foo", "bar"), UnmappedFieldsPattern.keepOrdered(List.of("foo", "bar"), List.of("foo", "*")));
        assertEquals(List.of("bar", "foo"), UnmappedFieldsPattern.keepOrdered(List.of("foo", "bar"), List.of("bar*", "foo", "*")));
        assertEquals(List.of("bar", "foo"), UnmappedFieldsPattern.keepOrdered(List.of("foo", "bar"), List.of("foo*", "bar", "fo*")));
    }

    /**
     * The LOAD_ALL case this ordering exists for: the real columns and runtime-discovered leaves (alphabetical in {@code childOutput})
     * are reordered to honor KEEP's left-to-right contract - including moving a real column ({@code id}) after the expanded leaves.
     */
    public void testKeepOrderedInterleavesRealColumnsAndExpandedLeaves() {
        List<String> childOutput = List.of("id", "unmapped", "unmapped.bar", "unmapped.deep.leaf", "unmapped.foo");
        assertEquals(
            List.of("id", "unmapped.bar", "unmapped.deep.leaf", "unmapped.foo", "unmapped"),
            UnmappedFieldsPattern.keepOrdered(childOutput, List.of("*", "unmapped.*", "unmapped"))
        );
        assertEquals(
            List.of("unmapped.bar", "unmapped.deep.leaf", "unmapped.foo", "unmapped", "id"),
            UnmappedFieldsPattern.keepOrdered(childOutput, List.of("unmapped.*", "unmapped", "*"))
        );
        // An explicit real column pinned between two leaf-producing terms: id lands after unmapped.* leaves but before the scalar unmapped.
        assertEquals(
            List.of("unmapped.bar", "unmapped.deep.leaf", "unmapped.foo", "id", "unmapped"),
            UnmappedFieldsPattern.keepOrdered(childOutput, List.of("unmapped.*", "id", "unmapped"))
        );
    }

    /**
     * A {@code childOutput} name that no term matches - e.g. a column an {@code EVAL} added above the governing {@code KEEP} - keeps its
     * natural trailing position, so reordering never drops a column.
     */
    public void testKeepOrderedAppendsUnmatchedColumnsLast() {
        assertEquals(List.of("unmapped.foo", "x"), UnmappedFieldsPattern.keepOrdered(List.of("x", "unmapped.foo"), List.of("unmapped.*")));
    }
}
