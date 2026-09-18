/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;

public interface OverspillAssignments {

    OverspillAssignments NONE = new OverspillAssignments() {
        @Override
        public int size() {
            return 0;
        }

        @Override
        public PrimitiveIterator.OfInt getAssignmentsFor(int ordinal) {
            return EMPTY_ITERATOR;
        }
    };

    PrimitiveIterator.OfInt EMPTY_ITERATOR = new PrimitiveIterator.OfInt() {
        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public int nextInt() {
            throw new NoSuchElementException();
        }
    };

    /**
     * The number of vectors with overspill information.
     * Note that some vectors may have no overspill assignments - they are still counted here.
     */
    int size();

    /**
     * Returns an iterator over the additional centroid indices (if any) for the given vector ordinal.
     * The returned iterator will be empty if the ordinal has no overspill assignments,
     * or the ordinal is out of range (@code{ordinal >= size}) for this instance.
     */
    PrimitiveIterator.OfInt getAssignmentsFor(int ordinal);
}
