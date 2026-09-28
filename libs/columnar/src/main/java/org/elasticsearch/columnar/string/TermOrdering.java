/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.IntroSorter;

import java.util.function.IntBinaryOperator;

/**
 * Orders term ids in place. The term decides where the caller's rule does not, so the ordering is total and
 * a column yields the same answer however its values arrived.
 */
final class TermOrdering {

    private final BytesRefHash terms;

    TermOrdering(BytesRefHash terms) {
        this.terms = terms;
    }

    void sort(int[] ids, int from, int to, IntBinaryOperator rule) {
        order(ids, from, to, rule);
    }

    void byTerm(int[] ids, int from, int to) {
        order(ids, from, to, null);
    }

    private void order(int[] ids, int from, int to, IntBinaryOperator rule) {
        new IntroSorter() {
            private final BytesRef left = new BytesRef();
            private final BytesRef right = new BytesRef();
            private int pivotId;

            @Override
            protected void swap(int i, int j) {
                final int tmp = ids[i];
                ids[i] = ids[j];
                ids[j] = tmp;
            }

            @Override
            protected int compare(int i, int j) {
                return compareIds(ids[i], ids[j]);
            }

            @Override
            protected void setPivot(int i) {
                pivotId = ids[i];
            }

            @Override
            protected int comparePivot(int j) {
                return compareIds(pivotId, ids[j]);
            }

            private int compareIds(int a, int b) {
                if (rule != null) {
                    final int cmp = rule.applyAsInt(a, b);
                    if (cmp != 0) {
                        return cmp;
                    }
                }
                terms.get(a, left);
                terms.get(b, right);
                return left.compareTo(right);
            }
        }.sort(from, to);
    }
}
