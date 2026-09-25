/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.fixtures;

import java.util.ArrayList;
import java.util.List;

/**
 * One slice of a suite's registered cases, so a crossing too large for a single JVM can be spread
 * across pipeline steps.
 *
 * <p>The unit is the <b>registered case</b>, not the vector, and the difference is the whole reason
 * this type exists. Vectors carry wildly unequal case counts -- a directive-only vector crosses the
 * entire routed corpus while a dialect vector is confined to the standalone datasets by
 * {@code bytesCannotCarry} -- so slicing the vector list produces shards whose real cost differs by
 * several times over. Slicing the case rows balances every shard to within one case, and it does so
 * without knowing anything about what a vector is.
 *
 * <p>Round-robin rather than contiguous blocks, for the same reason: the crossing emits all of one
 * spec's cases together, so a contiguous slice would hand one shard a whole expensive spec and
 * another a whole cheap one. Case {@code k} belongs to shard {@code (k mod count) + 1}.
 *
 * <p>Sharding is applied <em>after</em> the registration filters and the spec exclusions, over the
 * rows that would actually run. Filtering after sharding would unbalance the shards by exactly the
 * filters' per-vector bias, which is the bias this type exists to avoid.
 *
 * <p>Vector names are untouched by sharding, so an exclusion entry and a reproduce line mean the
 * same thing whatever shard produced them, and {@link #ALL} -- the default when nothing is set --
 * selects every case, which is what a bare local run wants.
 */
public record CaseShard(int index, int count) {

    /** Every case: the default, and what a local run without the system property gets. */
    public static final CaseShard ALL = new CaseShard(1, 1);

    public CaseShard {
        if (count < 1) {
            throw new IllegalArgumentException("shard count must be at least 1 but was [" + count + "]");
        }
        if (index < 1 || index > count) {
            throw new IllegalArgumentException("shard index must be in [1, " + count + "] but was [" + index + "]");
        }
    }

    /**
     * Parses the {@code i/S} spelling, one-based and inclusive, as it appears on a pipeline command line.
     *
     * <p>One-based because the value is written by hand in a yml matrix and read by a human in a build
     * name; {@code 1/6} through {@code 6/6} is what a step list looks like, and a {@code 0/6} in that
     * list is a typo rather than the first shard. It is rejected rather than interpreted.
     */
    public static CaseShard parse(String spec) {
        if (spec == null) {
            throw new IllegalArgumentException("shard spec must not be null; omit the property to run every case");
        }
        String trimmed = spec.trim();
        int slash = trimmed.indexOf('/');
        if (slash < 0 || trimmed.indexOf('/', slash + 1) >= 0) {
            throw new IllegalArgumentException("shard spec must be [index/count] but was [" + spec + "]");
        }
        return new CaseShard(parsePart(trimmed.substring(0, slash), spec), parsePart(trimmed.substring(slash + 1), spec));
    }

    private static int parsePart(String part, String spec) {
        try {
            return Integer.parseInt(part.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("shard spec must be [index/count] with integer parts but was [" + spec + "]", e);
        }
    }

    /** Whether the case at this zero-based position in the suite's registered list belongs to this shard. */
    public boolean selects(int caseIndex) {
        return Math.floorMod(caseIndex, count) == index - 1;
    }

    /**
     * This shard's cases, in the order they were registered.
     *
     * <p>Returns the input itself when there is only one shard, so the overwhelmingly common local run
     * pays nothing and cannot be reordered by a slicing bug that only fires when nobody is sharding.
     */
    public <T> List<T> select(List<T> cases) {
        if (count == 1) {
            return cases;
        }
        List<T> selected = new ArrayList<>((cases.size() / count) + 1);
        for (int i = 0; i < cases.size(); i++) {
            if (selects(i)) {
                selected.add(cases.get(i));
            }
        }
        return selected;
    }

    @Override
    public String toString() {
        return index + "/" + count;
    }
}
