/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.textstructure.structurefinder;

import org.elasticsearch.test.ESTestCase;

import java.util.List;

public class ImmutableLeavesTrieTests extends ESTestCase {

    public void testAdd_noConflicts() {
        ImmutableLeavesTrie trie = new ImmutableLeavesTrie();
        trie.add(List.of("a", "b", "c"));

        assertNull(trie.findConflict(List.of("x", "y")));
        assertTrue(trie.add(List.of("x", "y")));

        assertNull(trie.findConflict(List.of("a", "b", "d")));
        assertTrue(trie.add(List.of("a", "b", "d")));

        assertNull(trie.findConflict(List.of("a", "x", "y")));
        assertTrue(trie.add(List.of("a", "x", "y")));
    }

    public void testAdd_emptyList() {
        ImmutableLeavesTrie trie = new ImmutableLeavesTrie();
        trie.add(List.of("a", "b"));

        assertNull(trie.findConflict(List.of()));
        assertTrue(trie.add(List.of()));
    }

    public void testAdd_succeedsForExistingLeaf() {
        ImmutableLeavesTrie trie = new ImmutableLeavesTrie();
        trie.add(List.of("a", "b", "c"));

        assertNull(trie.findConflict(List.of("a", "b", "c")));
        assertTrue(trie.add(List.of("a", "b", "c")));
    }

    public void testAdd_failsWhenExtendingPastLeaf() {
        ImmutableLeavesTrie trie = new ImmutableLeavesTrie();
        trie.add(List.of("a", "b", "c"));

        var conflictingBranch = List.of("a", "b", "c", "d");
        List<String> conflict = trie.findConflict(conflictingBranch);
        assertEquals(List.of("a", "b", "c"), conflict);
        assertFalse(trie.add(conflictingBranch));
    }

    public void testAdd_failsExtendingPastLeaf2() {
        ImmutableLeavesTrie trie = new ImmutableLeavesTrie();
        assertTrue(trie.add(List.of("a", "b", "c")));

        var conflictingBranch = List.of("a", "b", "c", "d", "e", "f");
        assertFalse(trie.add(conflictingBranch));
        List<String> conflict = trie.findConflict(conflictingBranch);
        assertEquals(List.of("a", "b", "c"), conflict);
    }

    public void testAdd_failsOnExistingBranch1() {
        ImmutableLeavesTrie trie = new ImmutableLeavesTrie();
        trie.add(List.of("a", "b", "c", "d"));

        var conflictingBranch = List.of("a");
        assertFalse(trie.add(conflictingBranch));
        List<String> conflict = trie.findConflict(conflictingBranch);
        assertEquals(conflictingBranch, conflict);
    }

    public void testAdd_failsOnExistingBranch2() {
        ImmutableLeavesTrie trie = new ImmutableLeavesTrie();
        trie.add(List.of("a", "b", "c", "d"));

        var conflictingBranch = List.of("a", "b", "c");
        assertFalse(trie.add(conflictingBranch));
        List<String> conflict = trie.findConflict(conflictingBranch);
        assertEquals(conflictingBranch, conflict);
    }

}
