/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.textstructure.structurefinder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A Trie-like data structure that has two types of nodes: value nodes and object nodes.
 * Value nodes can store values, and object nodes can have other nodes as children.
 *
 * Existing nodes cannot be converted to another type.
 * For example, for a trie containing the path ["a", "b", "c"], where "c" is a value node:
 * - attempting to add a value to ["a", "b"] will fail because "b" is an object node, and it can't be turned into a value node
 * - attempting to add a value to ["a", "b", "d"] will succeed because "d" can be added as a child of "b", which is an object node
 * - attempting to add a value to ["a", "b", "c", "e"] will fail because "c" is a value node, and it can't have other nodes as children
 */
public class ObjectValueTrie {

    private final Node root = Node.createObjectNode();

    /**
     * Attempts to add a path to the trie with a value, the last element of the path will be a value node,
     * the other elements will be object nodes.
     *
     * @param path The path, starting from the root and ending at the value node.
     * @param value The value to store at the value node.
     * @return null if added successfully, or the conflict path if the new path cannot be added.
     */
    public List<String> tryAdd(List<String> path, Object value) {
        if (path.isEmpty()) {
            return null;
        }

        Node currentNode = root;
        for (int i = 0; i < path.size(); i++) {
            String nextNodeKey = path.get(i);
            Node nextNode = currentNode.getChild(nextNodeKey);
            boolean isLastElement = (i == path.size() - 1);

            if (isLastElement) {
                if (nextNode == null) {
                    Node newNode = Node.createValueNode(value);
                    currentNode.addChild(nextNodeKey, newNode);
                    return null;
                }

                if (nextNode.isObjectNode()) {
                    return path.subList(0, i + 1);
                }

                nextNode.addValue(value);
                return null;
            }

            if (nextNode == null) {
                nextNode = Node.createObjectNode();
                currentNode.addChild(nextNodeKey, nextNode);
            } else if (nextNode.isValueNode()) {
                return path.subList(0, i + 1);
            }
            currentNode = nextNode;
        }
        return null;
    }

    /**
     * Attempts to add a path to the trie and mark the last element of the path as an object node.
     *
     * @param path The path, starting from the root
     * @return null if marked successfully, or the conflict path if there's an existing value node in the path
     */
    public List<String> markAsObject(List<String> path) {
        if (path.isEmpty()) {
            return null;
        }

        Node currentNode = root;
        for (int i = 0; i < path.size(); i++) {
            String nextNodeKey = path.get(i);
            Node nextNode = currentNode.getChild(nextNodeKey);
            boolean isLastElement = (i == path.size() - 1);

            if (isLastElement) {
                if (nextNode == null) {
                    currentNode.addChild(nextNodeKey, Node.createObjectNode());
                    return null;
                }

                if (nextNode.isValueNode()) {
                    return path.subList(0, i + 1);
                }

                return null;
            }

            if (nextNode == null) {
                nextNode = Node.createObjectNode();
                currentNode.addChild(nextNodeKey, nextNode);
            } else if (nextNode.isValueNode()) {
                return path.subList(0, i + 1);
            }
            currentNode = nextNode;
        }
        return null;
    }

    private static final String DOT_DELIMITER = ".";

    /**
     * Flattens the trie into a map where keys are dot-delimited paths and values are lists of stored values.
     *
     * @return a map from dot-delimited paths to their values
     */
    public Map<String, List<Object>> flatten() {
        Map<String, List<Object>> result = new HashMap<>();
        flattenNode(root, new ArrayList<>(), result);
        return result;
    }

    private void flattenNode(Node node, List<String> currentPath, Map<String, List<Object>> result) {
        if (node.isValueNode() && node.getValues().isEmpty() == false) {
            String key = String.join(DOT_DELIMITER, currentPath);
            result.put(key, new ArrayList<>(node.getValues()));
        } else if (node.isObjectNode() && node.hasChildren() == false && currentPath.isEmpty() == false) {
            // Empty object with no children - include as [{}]
            String key = String.join(DOT_DELIMITER, currentPath);
            List<Object> emptyMapList = new ArrayList<>();
            emptyMapList.add(new HashMap<>());
            result.put(key, emptyMapList);
        }

        if (node.isObjectNode()) {
            for (Map.Entry<String, Node> entry : node.getChildren().entrySet()) {
                currentPath.add(entry.getKey());
                flattenNode(entry.getValue(), currentPath, result);
                currentPath.removeLast();
            }
        }
    }

    /**
     * A node in the trie. Can either store values or have other nodes as children, but not both.
     */
    private static class Node {

        private List<Object> values;
        private Map<String, Node> children;

        private Node() {}

        static Node createObjectNode() {
            Node node = new Node();
            node.children = new HashMap<>();
            return node;
        }

        static Node createValueNode(Object value) {
            Node node = new Node();
            node.values = new ArrayList<>();
            node.values.add(value);
            return node;
        }

        boolean isObjectNode() {
            return children != null;
        }

        boolean isValueNode() {
            return values != null;
        }

        boolean hasChildren() {
            return children != null && children.isEmpty() == false;
        }

        Node getChild(String key) {
            return children != null ? children.get(key) : null;
        }

        void addChild(String key, Node child) {
            if (isObjectNode() == false) {
                throw new IllegalStateException("Node is not an object node, it can't have children");
            }
            children.put(key, child);
        }

        void addValue(Object value) {
            if (isValueNode() == false) {
                throw new IllegalStateException("Node is not a value node, it can't store values");
            }
            values.add(value);
        }

        List<Object> getValues() {
            return values;
        }

        Map<String, Node> getChildren() {
            return children;
        }
    }
}
