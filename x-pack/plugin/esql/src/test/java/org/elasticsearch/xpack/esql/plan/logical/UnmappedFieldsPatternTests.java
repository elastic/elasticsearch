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

    /**
     * The data-node object push ({@link UnmappedFieldsPattern#matchesObjectPush}) prunes a whole object only when an exclude provably
     * covers its entire subtree: a wildcard that matches the parent name <em>and ends in {@code *}</em>, so its trailing wildcard also
     * absorbs every {@code .child}. An exact exclude names a single column (not the subtree), and a fixed-suffix wildcard like {@code *ped}
     * matches the parent {@code unmapped} but misses {@code unmapped.deep.leaf} — so neither prunes here; the object still ships and the
     * coordinator decides its leaves per name. A nested-wildcard drop targets a descendant subtree, not the parent, so it too ships.
     */
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

    /**
     * The object push ignores includes entirely: an object always ships unless a wildcard exclude covers it, and which of its leaves
     * survive a {@code KEEP} is decided per leaf by {@link UnmappedFieldsPattern#matches} on the coordinator (see
     * {@link #testMatchesGovernsDottedLeavesForSourceParity}).
     */
    public void testObjectPushIgnoresIncludes() {
        UnmappedFieldsPattern keepExact = UnmappedFieldsPattern.includes(List.of("network")).withAdditionalExcludes(List.of("network"));
        assertTrue(keepExact.matchesObjectPush("network"));
        assertTrue(keepExact.matchesObjectPush("unrelated"));

        UnmappedFieldsPattern keepWildcard = UnmappedFieldsPattern.includes(List.of("keep*"));
        assertTrue(keepWildcard.matchesObjectPush("keep_me"));
        assertTrue(keepWildcard.matchesObjectPush("other"));
    }

    /**
     * The coordinator tests each flattened dotted leaf with {@link UnmappedFieldsPattern#matches}, exactly as a stored source's literal
     * dotted key would be tested. These leaf-level decisions are what give synthetic and stored source parity: an exact exclude of the
     * parent leaves the descendants alone, a child wildcard keeps them, a nested-wildcard drop removes only its subtree, and an exact
     * include of the parent does not pull in the descendants.
     */
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
}
