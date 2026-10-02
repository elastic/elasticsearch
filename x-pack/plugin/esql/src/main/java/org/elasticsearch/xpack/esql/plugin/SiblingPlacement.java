/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.xpack.esql.plan.physical.MergeExec;

/**
 * Where this producer sits among the children of its nearest enclosing merge.
 * <p>
 * {@code index} and {@code count} rotate assignment for every merge kind. {@code sourceSiblings}
 * is true only for a {@link MergeExec.Kind#UNION} leaf: those children are distinct datasets, so a
 * single-split producer is worth hopping so the coordinator is not asked to decode every file.
 * {@link MergeExec.Kind#FORK} branches re-read the same dataset and never count as source siblings,
 * even though they still rotate so concurrent re-reads spread across nodes.
 */
public record SiblingPlacement(int index, int count, boolean sourceSiblings) {

    public static final SiblingPlacement SINGLE = new SiblingPlacement(0, 1, false);

    public SiblingPlacement {
        if (index < 0) {
            throw new IllegalArgumentException("index must not be negative");
        }
        if (count < 1) {
            throw new IllegalArgumentException("count must be at least one");
        }
        if (index >= count) {
            throw new IllegalArgumentException("index [" + index + "] must be less than count [" + count + "]");
        }
    }

    /**
     * Placement for child {@code index} of a merge with {@code count} children. UNION leaves are
     * source siblings; FORK branches rotate but are not.
     */
    public static SiblingPlacement forMerge(MergeExec.Kind kind, int index, int count) {
        return switch (kind) {
            case FORK -> new SiblingPlacement(index, count, false);
            case UNION -> new SiblingPlacement(index, count, true);
        };
    }

    /**
     * True only for a UNION leaf with at least one sibling. Drives the Adaptive single-split gate.
     */
    public boolean hasSourceSiblings() {
        return sourceSiblings && count > 1;
    }

    /**
     * Offset into the eligible-node ring. Siblings place independently, so a sibling with fewer
     * splits than an earlier one may share nodes with it.
     */
    public int stride(int splitCount, int nodeCount) {
        return Math.floorMod((long) index * Math.max(splitCount, 1), nodeCount);
    }
}
