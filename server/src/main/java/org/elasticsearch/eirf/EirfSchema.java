/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.eirf;

import com.carrotsearch.hppc.ObjectIntHashMap;
import com.carrotsearch.hppc.ObjectIntMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Schema for the Elastic Internal Row Format (EIRF).
 *
 * <p>Uses a parent-pointer structure with two levels:
 * <ul>
 *   <li><b>Non-leaf fields</b> (objects/containers) form a tree. Index 0 is always the root.</li>
 *   <li><b>Leaf fields</b> (columns in row data) each point to a parent non-leaf field.</li>
 * </ul>
 *
 * <p>Example for {@code {"user": {"name": "alice"}, "status": "active"}}:
 * <pre>
 * Non-leaf: [root(parent:-1), "user"(parent:0)]
 * Leaf:     ["name"(parent:1), "status"(parent:0)]
 * </pre>
 */
public final class EirfSchema {

    private static final int INITIAL_CAPACITY = 8;
    /** Maximum number of fields per level, constrained by u16 encoding in the batch header. */
    static final int MAX_FIELDS = 65535;

    private final FieldLevel nonLeaves;
    private final FieldLevel leaves;

    /**
     * Creates a new schema with root automatically added as non-leaf index 0.
     */
    public EirfSchema() {
        this.nonLeaves = new FieldLevel(INITIAL_CAPACITY);
        this.leaves = new FieldLevel(INITIAL_CAPACITY);

        // Add root at index 0, self-referential parent
        nonLeaves.append("", 0);
    }

    /**
     * Constructor for reading: builds from pre-parsed non-leaf and leaf arrays.
     */
    EirfSchema(List<String> nonLeafNames, int[] nonLeafParents, List<String> leafNames, int[] leafParents) {
        this.nonLeaves = new FieldLevel(nonLeafNames, nonLeafParents);
        this.leaves = new FieldLevel(leafNames, leafParents);
    }

    public int nonLeafCount() {
        return nonLeaves.count();
    }

    public String getNonLeafName(int idx) {
        return nonLeaves.getName(idx);
    }

    public int getNonLeafParent(int idx) {
        return nonLeaves.getParent(idx);
    }

    /**
     * Finds a non-leaf field by name and parent index. Returns -1 if not found.
     */
    public int findNonLeaf(String name, int parentIdx) {
        return nonLeaves.find(name, parentIdx);
    }

    /**
     * Appends a non-leaf field if not already present. Idempotent.
     */
    public int appendNonLeaf(String name, int parentIdx) {
        return nonLeaves.append(name, parentIdx);
    }

    /**
     * Returns the number of leaf fields (columns).
     */
    public int leafCount() {
        return leaves.count();
    }

    public String getLeafName(int idx) {
        return leaves.getName(idx);
    }

    public int getLeafParent(int idx) {
        return leaves.getParent(idx);
    }

    /**
     * Finds a leaf field by name and parent non-leaf index. Returns -1 if not found.
     */
    public int findLeaf(String name, int parentIdx) {
        return leaves.find(name, parentIdx);
    }

    /**
     * Appends a leaf field if not already present. Idempotent.
     */
    public int appendLeaf(String name, int parentIdx) {
        return leaves.append(name, parentIdx);
    }

    /**
     * Reconstructs the full dot-separated path for a leaf field by walking parent pointers.
     * For a leaf "name" under non-leaf "user" under root, returns "user.name".
     * For a leaf "status" directly under root, returns "status".
     */
    public String getFullPath(int leafIdx) {
        String leafName = leaves.getName(leafIdx);
        int parentIdx = leaves.getParent(leafIdx);

        if (parentIdx == 0) {
            return leafName;
        }

        StringBuilder sb = new StringBuilder();
        buildNonLeafPath(sb, parentIdx);
        sb.append('.').append(leafName);
        return sb.toString();
    }

    private void buildNonLeafPath(StringBuilder sb, int nonLeafIdx) {
        if (nonLeafIdx == 0) {
            return;
        }
        int parent = nonLeaves.getParent(nonLeafIdx);
        buildNonLeafPath(sb, parent);
        if (sb.isEmpty() == false) {
            sb.append('.');
        }
        sb.append(nonLeaves.getName(nonLeafIdx));
    }

    /**
     * Returns the chain of non-leaf indices from root to the given non-leaf index (inclusive).
     * Root (index 0) is excluded from the result.
     */
    int[] getNonLeafChain(int nonLeafIdx) {
        if (nonLeafIdx == 0) {
            return new int[0];
        }
        int depth = 0;
        int idx = nonLeafIdx;
        while (idx != 0) {
            depth++;
            idx = nonLeaves.getParent(idx);
        }
        int[] chain = new int[depth];
        idx = nonLeafIdx;
        for (int i = depth - 1; i >= 0; i--) {
            chain[i] = idx;
            idx = nonLeaves.getParent(idx);
        }
        return chain;
    }

    private record FieldKey(int parentIdx, String name) {}

    /**
     * Holds a parallel name list, parent array, and lookup map for one level of schema fields.
     */
    private static final class FieldLevel {
        public static final int MISSING = -1;
        private final List<String> names;
        private int[] parents;
        private final ObjectIntMap<FieldKey> lookup;

        FieldLevel(int initialCapacity) {
            this.names = new ArrayList<>();
            this.parents = new int[initialCapacity];
            this.lookup = new ObjectIntHashMap<>(initialCapacity);
        }

        FieldLevel(List<String> names, int[] parents) {
            this.names = new ArrayList<>(names);
            this.parents = Arrays.copyOf(parents, names.size());
            this.lookup = new ObjectIntHashMap<>(names.size());
            for (int i = 0; i < names.size(); i++) {
                lookup.put(new FieldKey(parents[i], names.get(i)), i);
            }
        }

        int count() {
            return names.size();
        }

        String getName(int idx) {
            return names.get(idx);
        }

        int getParent(int idx) {
            return parents[idx];
        }

        int find(String name, int parentIdx) {
            return lookup.getOrDefault(new FieldKey(parentIdx, name), MISSING);
        }

        int append(String name, int parentIdx) {
            FieldKey key = new FieldKey(parentIdx, name);
            int existing = lookup.getOrDefault(key, MISSING);
            if (existing != MISSING) {
                return existing;
            }
            int index = names.size();
            if (index >= MAX_FIELDS) {
                throw new IllegalStateException("Schema field count exceeds maximum of " + MAX_FIELDS);
            }
            names.add(name);
            if (index >= parents.length) {
                parents = Arrays.copyOf(parents, parents.length << 1);
            }
            parents[index] = parentIdx;
            lookup.put(key, index);
            return index;
        }
    }
}
