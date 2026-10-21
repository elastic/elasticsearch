/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class IndexSortRangeQueryNestedDocsCombinationsTests extends IndexSortRangeQueryNestedDocsTestCase {

    private static final List<Block> BLOCKS = List.of(
        new Block(0, 10L, 1),
        new Block(1, null, 1),
        new Block(2, 20L, 1),
        new Block(3, 30L, 2)
    );

    private final Kind kind;
    private final SortRole role;
    private final String order;
    private final String missing;
    private final boolean points;

    public IndexSortRangeQueryNestedDocsCombinationsTests(
        @Name("kind") String kind,
        @Name("role") SortRole role,
        @Name("order") String order,
        @Name("missing") String missing,
        @Name("points") boolean points
    ) {
        this.kind = kind(kind);
        this.role = role;
        this.order = order;
        this.missing = missing;
        this.points = points;
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        final List<Object[]> parameters = new ArrayList<>();
        for (final Kind kind : KINDS) {
            for (final SortRole role : SortRole.values()) {
                for (final String order : List.of("asc", "desc")) {
                    for (final String missing : role == SortRole.PRIMARY ? List.of("_first", "_last") : List.of("_last")) {
                        for (final boolean points : kind.supportsPoints() ? List.of(false, true) : List.of(false)) {
                            parameters.add(new Object[] { kind.type(), role, order, missing, points });
                        }
                    }
                }
            }
        }
        return parameters;
    }

    public void testRangeMatchesOnlyParentsWithAValueInRange() throws IOException {
        assertEquals(new Result(Set.of(2), 1), searchRange(20L, 20L));
        assertEquals(new Result(Set.of(0, 2, 3), 3), searchRange(0L, 100L));
    }

    private Result searchRange(long lower, long upper) throws IOException {
        return search(kind, role, points, order, missing, "min", BLOCKS, Set.of(), lower, upper, true, true);
    }
}
