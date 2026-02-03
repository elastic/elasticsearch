/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.textstructure.structurefinder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A Trie-like data structure with immutable leaves.
 * For example, say we have a trie with the following path: ["a", "b", "c"], where "c" is a leaf:
 * - attempting to add ["a", "f"] will fail because "a" is already a non-leaf node, and it can't be turned into a leaf
 * - attempting to add ["a", "b", "c", "d"] will fail because "c" is already a leaf node, and it can't be turned into a non-leaf node
 * - attempting to add ["a", "b", "e"] will succeed because "b" is a non-leaf node and "e" can be added as another child of "b"
 */
public class ImmutableLeavesTrie {

    private final Node root = new Node();

    /**
     * Adds a path to the trie, making the final element as a leaf.
     * @param path The path, starting from the root and ending at the leaf.
     * @return true if added successfully or already exists as leaf, false if there's a conflict
     */
    public boolean add(List<String> path) {
        if (path.isEmpty()) {
            return true;
        }

        Node currentNode = root;
        for (int i = 0; i < path.size(); i++) {
            String nextNodeKey = path.get(i);
            Node nextNode = currentNode.children.get(nextNodeKey);
            boolean isLastElement = (i == path.size() - 1);

            if (isLastElement) {
                if (nextNode == null) {
                    currentNode.children.put(nextNodeKey, new Node(true));
                    return true;
                }
                return nextNode.isLeaf();
            }

            if (nextNode == null) {
                nextNode = new Node();
                currentNode.children.put(nextNodeKey, nextNode);
            } else if (nextNode.isLeaf()) {
                return false;
            }
            currentNode = nextNode;
        }
        return true;
    }

    /**
     * Finds where a conflict would occur if we tried to add the given path.
     * @param path The path, starting from the root and ending at the leaf.
     * @return the path prefix where the conflict occurs, or null if no conflict.
     * For example, if the trie has the path ["a", "b", "c"]
     * - if we try to add ["a", "b"], the conflicting path will be ["a", "b"].
     * - if we try to add ["a", "b", "c", "d"], the conflicting path will be ["a", "b", "c"].
     */
    public List<String> findConflict(List<String> path) {
        if (path.isEmpty()) {
            return null;
        }

        Node currentNode = root;
        for (int i = 0; i < path.size(); i++) {
            String nextNodeKey = path.get(i);
            Node nextNode = currentNode.children.get(nextNodeKey);
            boolean isLastElement = (i == path.size() - 1);

            if (isLastElement) {
                if (nextNode == null || nextNode.isLeaf()) {
                    return null;
                }
                return List.copyOf(path.subList(0, i + 1));
            }

            if (nextNode == null) {
                return null;
            }
            if (nextNode.isLeaf()) {
                return List.copyOf(path.subList(0, i + 1));
            }
            currentNode = nextNode;
        }
        return null;
    }

    private static class Node {
        private final Map<String, Node> children = new HashMap<>();
        private final boolean isLeaf;

        private Node(boolean isLeaf) {
            this.isLeaf = isLeaf;
        }

        private Node() {
            this.isLeaf = false;
        }

        boolean isLeaf() {
            return isLeaf;
        }
    }
}
