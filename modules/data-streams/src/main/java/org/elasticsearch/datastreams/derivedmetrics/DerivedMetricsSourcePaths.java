/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.derivedmetrics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The set of {@code _source} paths a configuration reads, each assigned a slot, arranged as a trie for streaming extraction.
 *
 * <p>Slots are what make the write path cheap. Every path a metric needs — dimensions, predicate fields, value fields — is numbered once
 * at compile time, so the per-document work is filling an array indexed by slot rather than building a map and then looking paths up in
 * it. Nothing downstream carries a path string.
 *
 * <p>The trie exists so that extraction can decide, at each field it meets, whether anything below that field is wanted, and skip the
 * whole subtree when it is not. A document is mostly fields no metric cares about, so skipping is the common case.
 */
public final class DerivedMetricsSourcePaths {

    /**
     * A node of the trie. {@code slot} is non-negative only where a configured path ends, so a node can be both a value that is wanted and
     * a step towards deeper ones — {@code host} and {@code host.name} can both be configured.
     */
    static final class Node {
        private final Map<String, Node> children = new HashMap<>();
        private int slot = -1;

        Node child(String name) {
            return children.get(name);
        }

        boolean hasChildren() {
            return children.isEmpty() == false;
        }

        int slot() {
            return slot;
        }
    }

    private final Node root = new Node();
    private final Map<String, Integer> slots = new HashMap<>();
    private final List<String> paths = new ArrayList<>();

    /**
     * The slot for a path, assigning one if this is the first time it has been asked for. Two metrics naming the same field share a slot,
     * so the value is extracted once per document however many metrics want it.
     */
    public int slotFor(String path) {
        Integer existing = slots.get(path);
        if (existing != null) {
            return existing;
        }
        int slot = paths.size();
        slots.put(path, slot);
        paths.add(path);

        Node node = root;
        for (String segment : path.split("\\.")) {
            node = node.children.computeIfAbsent(segment, unused -> new Node());
        }
        node.slot = slot;
        return slot;
    }

    /** How many distinct paths were assigned, which is the size of the array extraction fills. */
    public int size() {
        return paths.size();
    }

    public List<String> paths() {
        return List.copyOf(paths);
    }

    Node root() {
        return root;
    }
}
