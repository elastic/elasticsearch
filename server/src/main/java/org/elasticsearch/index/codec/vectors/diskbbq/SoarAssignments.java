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

import static org.elasticsearch.index.codec.vectors.cluster.Soar.NO_SOAR_ASSIGNMENT;

public record SoarAssignments(int[] assignments) implements OverspillAssignments {

    @Override
    public int size() {
        return assignments.length;
    }

    @Override
    public PrimitiveIterator.OfInt getAssignmentsFor(int ordinal) {
        if (assignments.length > ordinal && assignments[ordinal] != NO_SOAR_ASSIGNMENT) {
            return new SingleIterator(assignments[ordinal]);
        } else {
            return EMPTY_ITERATOR;
        }
    }

    private static final PrimitiveIterator.OfInt EMPTY_ITERATOR = new PrimitiveIterator.OfInt() {
        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public int nextInt() {
            throw new NoSuchElementException();
        }
    };

    private static class SingleIterator implements PrimitiveIterator.OfInt {
        private final int value;
        private boolean consumed;

        private SingleIterator(int value) {
            this.value = value;
        }

        @Override
        public boolean hasNext() {
            return !consumed;
        }

        @Override
        public int nextInt() {
            if (!consumed) {
                consumed = true;
                return value;
            } else {
                throw new NoSuchElementException();
            }
        }
    }
}
