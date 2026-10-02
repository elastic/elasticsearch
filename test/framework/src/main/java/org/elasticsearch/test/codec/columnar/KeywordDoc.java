/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.codec.columnar;

import org.elasticsearch.core.Nullable;

import java.util.List;
import java.util.Objects;

/**
 * One document in a keyword duel corpus. {@code docId} is a stable, retrievable ordinal used as the sort
 * tiebreak and as the identity compared across indices. {@code values} carries the keyword field content:
 * {@code null} means the field is absent, an empty list means an empty array, and a non-empty list may
 * itself contain {@code null} elements to represent inline nulls that Elasticsearch drops from doc values.
 */
public record KeywordDoc(long docId, @Nullable List<String> values) {

    public String id() {
        return "d" + docId;
    }

    /**
     * @return the distinct, non-null keyword values as Elasticsearch stores them in {@code SortedSetDocValues}:
     *         sorted and deduplicated. Returns an empty list when the field is absent or holds only nulls.
     */
    public List<String> sortedDistinctValues() {
        if (values == null) {
            return List.of();
        }
        return values.stream().filter(value -> value != null).distinct().sorted().toList();
    }

    /**
     * @return the non-null keyword values in source order, keeping duplicates, as the columnar modes store
     *         them in the document-order array. Returns an empty list when the field is absent or holds only
     *         nulls.
     */
    public List<String> nonNullValues() {
        if (values == null) {
            return List.of();
        }
        return values.stream().filter(Objects::nonNull).toList();
    }
}
