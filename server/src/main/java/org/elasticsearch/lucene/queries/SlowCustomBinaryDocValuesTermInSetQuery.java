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
import java.util.Set;

/**
 * A query for matching any value from a set of BytesRef values for a specific field.
 * <p>
 * This implementation is slow, because it potentially scans binary doc values for each document.
 */
public final class SlowCustomBinaryDocValuesTermInSetQuery extends AbstractBinaryDocValuesQuery {

    private final Set<BytesRef> terms;

    public SlowCustomBinaryDocValuesTermInSetQuery(String fieldName, Set<BytesRef> terms) {
        super(fieldName, terms::contains);

        // verify inputs
        Objects.requireNonNull(terms);
        if (terms.isEmpty()) {
            throw new IllegalArgumentException("terms must not be empty");
        }
        for (BytesRef term : terms) {
            if (term == null) {
                throw new IllegalArgumentException("terms must not contain null");
            }
        }

        this.terms = terms;
    }

    @Override
    protected float matchCost() {
        // SlowCustomBinaryDocValuesTermQuery uses 10 for a single term, and while we have multiple terms, they're contained with a
        // HashSet, whose look up time is O(1). So, we're only slightly slower than a single term.
        return 11;
    }

    @Override
    public String toString(String field) {
        StringBuilder sb = new StringBuilder("SlowCustomBinaryDocValuesTermInSetQuery(fieldName=");
        sb.append(field).append(",terms=[");
        boolean first = true;
        for (BytesRef term : terms) {
            if (first == false) {
                sb.append(",");
            }
            sb.append(term.utf8ToString());
            first = false;
        }
        sb.append("])");
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (sameClassAs(o) == false) {
            return false;
        }
        SlowCustomBinaryDocValuesTermInSetQuery that = (SlowCustomBinaryDocValuesTermInSetQuery) o;
        return Objects.equals(fieldName, that.fieldName) && Objects.equals(terms, that.terms);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), fieldName, terms);
    }
}
