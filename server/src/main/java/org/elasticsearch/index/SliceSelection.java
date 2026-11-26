/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;

import java.util.LinkedHashSet;
import java.util.List;

/**
 * The slices a request reads from a shard.
 *
 * @param kind  how the slices were selected
 * @param names the selected slices, without duplicates; empty unless {@code kind} is {@link Kind#NAMED}
 */
public record SliceSelection(Kind kind, List<String> names) {

    public enum Kind {
        /** No slice was selected. Every slice is read, unless the field mapper that builds a query needs them by name. */
        UNSPECIFIED,
        /** One or more slices were selected by name. */
        NAMED
    }

    public static final SliceSelection UNSPECIFIED = new SliceSelection(Kind.UNSPECIFIED, List.of());

    public SliceSelection {
        names = List.copyOf(names);
        if ((kind == Kind.NAMED) == names.isEmpty()) {
            throw new IllegalArgumentException("slice names must be present only for a named slice selection");
        }
    }

    /**
     * Selects the given slices by name. Duplicates are dropped, the first occurrence keeps its position.
     */
    public static SliceSelection of(List<String> names) {
        final LinkedHashSet<String> unique = new LinkedHashSet<>();
        for (String name : names) {
            final String value = name.trim();
            if (value.isEmpty()) {
                throw new IllegalArgumentException("[" + SliceIndexing.FIELD_NAME + "] cannot be blank");
            }
            unique.add(value);
        }
        if (unique.isEmpty()) {
            throw new IllegalArgumentException("[" + SliceIndexing.FIELD_NAME + "] cannot be blank");
        }
        return new SliceSelection(Kind.NAMED, List.copyOf(unique));
    }

    /**
     * Parses the form carried by shard-level requests: {@code null} when no slice was requested,
     * otherwise a comma-separated list of slice names.
     */
    public static SliceSelection fromSearchSlice(@Nullable String searchSlice) {
        if (searchSlice == null) {
            return UNSPECIFIED;
        }
        return of(List.of(Strings.splitStringByCommaToArray(searchSlice)));
    }

    /**
     * Inverse of {@link #fromSearchSlice}.
     */
    @Nullable
    public String toSearchSlice() {
        return switch (kind) {
            case UNSPECIFIED -> null;
            case NAMED -> String.join(",", names);
        };
    }

    /**
     * The routing value that targets the shards holding the selected slices, or {@code null} when every shard must be searched.
     */
    @Nullable
    public String toRouting() {
        return kind == Kind.NAMED ? String.join(",", names) : null;
    }

    /**
     * Whether only the documents of {@link #names()} may be read.
     */
    public boolean isRestricted() {
        return kind == Kind.NAMED;
    }
}
