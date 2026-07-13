/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.textstructure.structurefinder;

import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;

public class ObjectValueTrieTests extends ESTestCase {

    public void testTryAdd_noConflictsWhenAddingSeparatePath() {
        ObjectValueTrie trie = new ObjectValueTrie();
        assertNull(trie.tryAdd(List.of("a", "b", "c"), "v1"));
        assertNull(trie.tryAdd(List.of("d", "e", "f"), "v2"));
    }

    public void testTryAdd_noConflictsWhenBranchingOut() {
        ObjectValueTrie trie = new ObjectValueTrie();
        assertNull(trie.tryAdd(List.of("a", "b", "c"), "v1"));
        assertNull(trie.tryAdd(List.of("a", "b", "e"), "v2"));
    }

    public void testTryAdd_noConflictsWhenAddingToExistingValueNode() {
        ObjectValueTrie trie = new ObjectValueTrie();
        assertNull(trie.tryAdd(List.of("a", "b", "c"), "value"));
        assertNull(trie.tryAdd(List.of("a", "b", "c"), "value2"));
    }

    public void testTryAdd_returnsConflictWhenExtendingPastValueNode() {
        ObjectValueTrie trie = new ObjectValueTrie();
        assertNull(trie.tryAdd(List.of("a", "b", "c"), "value"));

        var conflictingBranch = List.of("a", "b", "c", "d", "e", "f");
        List<String> conflict = trie.tryAdd(conflictingBranch, "value2");
        assertEquals(List.of("a", "b", "c"), conflict);
    }

    public void testTryAdd_returnsConflictWhenAddingValueNodeOnExistingPath() {
        ObjectValueTrie trie = new ObjectValueTrie();
        assertNull(trie.tryAdd(List.of("a", "b", "c"), "value"));

        var conflictingBranch = List.of("a", "b");
        List<String> conflict = trie.tryAdd(conflictingBranch, "value2");
        assertEquals(conflictingBranch, conflict);
    }

    public void testMarkAsObject_allowsChildrenLater() {
        ObjectValueTrie trie = new ObjectValueTrie();
        assertNull(trie.markAsObject(List.of("a", "b")));

        assertNull(trie.tryAdd(List.of("a", "b", "c"), "value"));
    }

    public void testMarkAsObject_returnsConflictOnAddValueOnMarkedPath() {
        ObjectValueTrie trie = new ObjectValueTrie();
        assertNull(trie.markAsObject(List.of("a", "b")));

        List<String> conflict = trie.tryAdd(List.of("a", "b"), "concrete");
        assertEquals(List.of("a", "b"), conflict);
    }

    public void testMarkAsObject_returnsConflictOnMarkingExistingValueNode() {
        ObjectValueTrie trie = new ObjectValueTrie();
        assertNull(trie.tryAdd(List.of("a", "b"), "value"));

        List<String> conflict = trie.markAsObject(List.of("a", "b"));
        assertEquals(List.of("a", "b"), conflict);
    }

    public void testMarkAsObject_emptyList() {
        ObjectValueTrie trie = new ObjectValueTrie();
        assertNull(trie.markAsObject(List.of()));
    }

    public void testMarkAsObject_returnsConflictWhenExtendingPastValueNode() {
        ObjectValueTrie trie = new ObjectValueTrie();
        assertNull(trie.tryAdd(List.of("a", "b"), "value"));

        List<String> conflict = trie.markAsObject(List.of("a", "b", "c"));
        assertEquals(List.of("a", "b"), conflict);
    }

    public void testMarkAsObject_noConflictWhenMarkingExistingObjectNode() {
        ObjectValueTrie trie = new ObjectValueTrie();
        assertNull(trie.tryAdd(List.of("a", "b", "c"), "value"));

        assertNull(trie.markAsObject(List.of("a", "b")));
    }

    public void testFlatten_emptyTrie() {
        ObjectValueTrie trie = new ObjectValueTrie();
        Map<String, List<Object>> result = trie.flatten();
        assertTrue(result.isEmpty());
    }

    public void testFlatten_singlePath() {
        ObjectValueTrie trie = new ObjectValueTrie();
        trie.tryAdd(List.of("a", "b", "c"), "value");

        Map<String, List<Object>> result = trie.flatten();
        assertEquals(1, result.size());
        assertEquals(List.of("value"), result.get("a.b.c"));
    }

    public void testFlatten_multiplePaths() {
        ObjectValueTrie trie = new ObjectValueTrie();
        trie.tryAdd(List.of("a", "b"), "v1");
        trie.tryAdd(List.of("x", "y", "z"), "v2");
        trie.tryAdd(List.of("foo"), "v3");

        Map<String, List<Object>> result = trie.flatten();
        assertEquals(3, result.size());
        assertEquals(List.of("v1"), result.get("a.b"));
        assertEquals(List.of("v2"), result.get("x.y.z"));
        assertEquals(List.of("v3"), result.get("foo"));
    }

    public void testFlatten_multipleValuesAtSamePath() {
        ObjectValueTrie trie = new ObjectValueTrie();
        trie.tryAdd(List.of("a", "b"), "v1");
        trie.tryAdd(List.of("a", "b"), "v2");
        trie.tryAdd(List.of("a", "b"), "v3");

        Map<String, List<Object>> result = trie.flatten();
        assertEquals(1, result.size());
        assertEquals(List.of("v1", "v2", "v3"), result.get("a.b"));
    }

    public void testFlatten_markedObjectWithChildrenNotIncluded() {
        ObjectValueTrie trie = new ObjectValueTrie();
        trie.markAsObject(List.of("a", "b"));
        trie.tryAdd(List.of("a", "b", "c"), "value");

        Map<String, List<Object>> result = trie.flatten();
        assertEquals(1, result.size());
        assertEquals(List.of("value"), result.get("a.b.c"));
        assertNull(result.get("a.b"));
    }

    public void testFlatten_markedObjectWithNoChildrenIncludedAsEmptyMap() {
        ObjectValueTrie trie = new ObjectValueTrie();
        trie.markAsObject(List.of("a", "b"));

        Map<String, List<Object>> result = trie.flatten();
        assertEquals(1, result.size());
        assertNotNull(result.get("a.b"));
        assertEquals(1, result.get("a.b").size());
        assertTrue(result.get("a.b").get(0) instanceof Map);
        assertTrue(((Map<?, ?>) result.get("a.b").get(0)).isEmpty());
    }

    public void testFlatten_branchedPath() {
        ObjectValueTrie trie = new ObjectValueTrie();
        trie.tryAdd(List.of("a", "b", "c"), "v1");
        trie.tryAdd(List.of("a", "b", "d"), "v2");
        trie.tryAdd(List.of("a", "x"), "v3");

        Map<String, List<Object>> result = trie.flatten();
        assertEquals(3, result.size());
        assertEquals(List.of("v1"), result.get("a.b.c"));
        assertEquals(List.of("v2"), result.get("a.b.d"));
        assertEquals(List.of("v3"), result.get("a.x"));
    }

}
