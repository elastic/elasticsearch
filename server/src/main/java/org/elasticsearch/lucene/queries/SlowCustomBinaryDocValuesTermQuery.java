/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.queries;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.IndexSortConfig;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;

/**
 * A query for matching an exact BytesRef value for a specific field.
 * The equivalent of {@link org.apache.lucene.document.SortedDocValuesField#newSlowExactQuery(String, BytesRef)},
 * but then for binary doc values.
 * <p>
 * This implementation is slow, because it potentially scans binary doc values for each document.
 */
public final class SlowCustomBinaryDocValuesTermQuery extends AbstractBinaryDocValuesQuery {

    private final BytesRef term;

    public SlowCustomBinaryDocValuesTermQuery(String fieldName, BytesRef term, IndexSortConfig config) {
        super(fieldName, shouldSkip(config, fieldName) ? predicateWithSkip(term, config) : (value, iterator) -> value.equals(term));
        this.term = Objects.requireNonNull(term);
    }

    @Override
    protected float matchCost() {
        return 10; // because one comparison
    }

    @Override
    public String toString(String field) {
        return "SlowCustomBinaryDocValuesTermQuery(fieldName=" + field + ",term=" + term.utf8ToString() + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (sameClassAs(o) == false) {
            return false;
        }
        SlowCustomBinaryDocValuesTermQuery that = (SlowCustomBinaryDocValuesTermQuery) o;
        return Objects.equals(fieldName, that.fieldName) && Objects.equals(term, that.term);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), fieldName, term);
    }

    static boolean shouldSkip(IndexSortConfig config, String fieldName) {
        if (config.hasPrimarySortOnField(fieldName)) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * If filtering by a field that is the primary index sort field,
     * then skip to NO_MORE_DOCS when the found value is larger or smaller to requested value.
     */
    static PredicateWithIterator predicateWithSkip(BytesRef term, IndexSortConfig config) {
        boolean sortOrderDesc = config.getPrimarySortOrderDesc();
        if (sortOrderDesc) {
            return (value, iterator) -> {
                int cmp = value.compareTo(term);
                if (cmp < 0) {
                    try {
                        iterator.advance(DocIdSetIterator.NO_MORE_DOCS);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
                return cmp == 0;
            };
        } else {
            return (value, iterator) -> {
                int cmp = value.compareTo(term);
                if (cmp > 0) {
                    try {
                        iterator.advance(DocIdSetIterator.NO_MORE_DOCS);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
                return cmp == 0;
            };
        }
    }
}
