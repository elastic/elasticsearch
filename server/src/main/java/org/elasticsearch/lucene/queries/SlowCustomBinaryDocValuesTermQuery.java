/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.queries;

import org.apache.lucene.util.BytesRef;

import java.util.Objects;

/**
 * A query for matching an exact BytesRef value for a specific field.
 * The equavalent of {@link org.apache.lucene.document.SortedDocValuesField#newSlowExactQuery(String, BytesRef)},
 * but then for binary doc values.
 * <p>
 * This implementation is slow, because it potentially scans binary doc values for each document.
 */
public final class SlowCustomBinaryDocValuesTermQuery extends AbstractBinaryDocValuesQuery {

    private final BytesRef term;

    public SlowCustomBinaryDocValuesTermQuery(String fieldName, BytesRef term) {
        super(fieldName, term::equals);
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
}
