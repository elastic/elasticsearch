/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.stats;

import org.apache.lucene.util.BytesRef;

/**
 * Interface for determining information about fields in the index.
 * This is used by the optimizer to make decisions about how to optimize queries.
 */
public interface SearchStats {
    SearchStats EMPTY = new EmptySearchStats();

    boolean exists(String field);

    boolean isIndexed(String field);

    boolean hasDocValues(String field);

    boolean hasExactSubfield(String field);

    long count();

    long count(String field);

    long count(String field, BytesRef value);

    Object min(String field);

    Object max(String field);

    boolean isSingleValue(String field);

    boolean canUseEqualityOnSyntheticSourceDelegate(String name, String value);

    /**
     * Returns the value for a field if it's a constant (eg. a constant_keyword with only one value for the involved indices).
     * NULL if the field is not a constant.
     */
    default String constantValue(String name) {
        return null;
    }

    /**
     * When there are no search stats available, for example when there are no search contexts, we have static results.
     */
    record EmptySearchStats() implements SearchStats {

        @Override
        public boolean exists(String field) {
            return false;
        }

        @Override
        public boolean isIndexed(String field) {
            return false;
        }

        @Override
        public boolean hasDocValues(String field) {
            return false;
        }

        @Override
        public boolean hasExactSubfield(String field) {
            return false;
        }

        @Override
        public long count() {
            return 0;
        }

        @Override
        public long count(String field) {
            return 0;
        }

        @Override
        public long count(String field, BytesRef value) {
            return 0;
        }

        @Override
        public Object min(String field) {
            return null;
        }

        @Override
        public Object max(String field) {
            return null;
        }

        @Override
        public boolean isSingleValue(String field) {
            return true;
        }

        @Override
        public boolean canUseEqualityOnSyntheticSourceDelegate(String name, String value) {
            return false;
        }
    }
}
