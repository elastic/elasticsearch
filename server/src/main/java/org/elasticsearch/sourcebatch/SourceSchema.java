/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.sourcebatch;

import com.carrotsearch.hppc.ObjectIntHashMap;
import com.carrotsearch.hppc.ObjectIntMap;

import org.apache.lucene.util.UnicodeUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

/**
 * The field schema shared by every row in a {@link SourceBatch}, independent of the physical layout
 * (row-major or column-major) the batch is stored in.
 *
 * <p>Uses a parent-pointer structure with two levels:
 * <ul>
 *   <li><b>Non-leaf fields</b> (objects/containers) form a tree. Index 0 is always the root.</li>
 *   <li><b>Leaf fields</b> (columns in the batch) each point to a parent non-leaf field.</li>
 * </ul>
 *
 * <p>Example for {@code {"user": {"name": "alice"}, "status": "active"}}:
 * <pre>
 * Non-leaf: [root(parent:-1), "user"(parent:0)]
 * Leaf:     ["name"(parent:1), "status"(parent:0)]
 * </pre>
 */
public final class SourceSchema {

    private static final int INITIAL_CAPACITY = 8;
    /** Maximum number of fields per level, constrained by u16 encoding in the batch header. */
    static final int MAX_FIELDS = 65535;

    private final FieldLevel nonLeaves;
    private final FieldLevel leaves;

    /**
     * Tracks leaves that have been written at least once as an empty object
     * ({@link org.elasticsearch.escf.EscfRowBuffer#emptyObject}).
     */
    private final BitSet emptyObjectSeen = new BitSet();
    /**
     * Tracks leaves that have been written at least once as a real value (non-empty-object).
     * A leaf is considered "always empty-object" iff {@link #emptyObjectSeen} is set and
     * {@link #realValueSeen} is not — meaning the sequential path would also index no data for it.
     */
    private final BitSet realValueSeen = new BitSet();

    /**
     * Creates a new schema with root automatically added as non-leaf index 0.
     */
    public SourceSchema() {
        this.nonLeaves = new FieldLevel(INITIAL_CAPACITY);
        this.leaves = new FieldLevel(INITIAL_CAPACITY);

        // Add root at index 0, self-referential parent
        nonLeaves.append("", 0);
    }

    /**
     * Constructor for reading: builds from pre-parsed non-leaf and leaf arrays.
     */
    public SourceSchema(List<String> nonLeafNames, int[] nonLeafParents, List<String> leafNames, int[] leafParents) {
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
        // TODO: Could consider caching this in some type of field name object.
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
     * Records that {@code leafIdx} was written as an empty-object leaf in at least one row.
     * Called from {@link org.elasticsearch.escf.EscfRowBuffer#emptyObject}.
     */
    public void noteEmptyObject(int leafIdx) {
        emptyObjectSeen.set(leafIdx);
    }

    /**
     * Records that {@code leafIdx} was written as a non-empty-object leaf in at least one row.
     * Called from all value-writing methods in {@link org.elasticsearch.escf.EscfRowBuffer} except
     * {@link org.elasticsearch.escf.EscfRowBuffer#emptyObject}.
     */
    public void noteRealValue(int leafIdx) {
        realValueSeen.set(leafIdx);
    }

    /**
     * Returns {@code true} if {@code leafIdx} has only ever been written as an empty-object leaf
     * (i.e. {@link #noteEmptyObject} was called and {@link #noteRealValue} was never called).
     *
     * <p>This lets {@link org.elasticsearch.index.mapper.ShardBatchMapper} skip unmapped empty-object
     * leaves rather than falling back to sequential indexing — the sequential path also produces no
     * index writes for empty objects under {@code subobjects: DISABLED}.
     */
    public boolean isAlwaysEmptyObject(int leafIdx) {
        return emptyObjectSeen.get(leafIdx) && realValueSeen.get(leafIdx) == false;
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
            // Use a transient key for the lookup so it never escapes this method and stays eligible for scalar
            // replacement on the common hit path.
            int existing = lookup.getOrDefault(new FieldKey(parentIdx, name), MISSING);
            if (existing != MISSING) {
                return existing;
            }
            int index = names.size();
            if (index >= MAX_FIELDS) {
                throw new IllegalStateException("Schema field count exceeds maximum of " + MAX_FIELDS);
            }
            if (UnicodeUtil.calcUTF16toUTF8Length(name, 0, name.length()) > MAX_FIELDS) {
                throw new IllegalStateException("Schema field name exceeds maximum of " + MAX_FIELDS + " bytes: " + name);
            }
            names.add(name);
            if (index >= parents.length) {
                parents = Arrays.copyOf(parents, parents.length << 1);
            }
            parents[index] = parentIdx;
            lookup.put(new FieldKey(parentIdx, name), index);
            return index;
        }
    }
}
