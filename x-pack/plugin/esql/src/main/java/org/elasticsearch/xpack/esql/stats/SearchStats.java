/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.stats;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute.FieldName;
import org.elasticsearch.xpack.esql.core.type.DataType;

/**
 * Interface for determining information about fields in the index.
 * This is used by the optimizer to make decisions about how to optimize queries.
 */
public interface SearchStats {
    SearchStats EMPTY = new EmptySearchStats();

    boolean exists(FieldName field);

    boolean isIndexed(FieldName field);

    boolean hasDocValues(FieldName field);

    boolean hasExactSubfield(FieldName field);

    long count();

    long count(FieldName field);

    long count(FieldName field, BytesRef value);

    byte[] min(FieldName field, DataType dataType);

    byte[] max(FieldName field, DataType dataType);

    boolean isSingleValue(FieldName field);

    boolean canUseEqualityOnSyntheticSourceDelegate(FieldName name, String value);

    boolean matchQueryYieldsCandidateMatchesForEquality(String name);

    /**
     * Returns the value for a field if it's a constant (eg. a constant_keyword with only one value for the involved indices).
     * NULL if the field is not a constant.
     */
    default String constantValue(FieldAttribute.FieldName name) {
        return null;
    }

    /**
     * When there are no search stats available, for example when there are no search contexts, we have static results.
     */
    record EmptySearchStats() implements SearchStats {

        @Override
        public boolean exists(FieldName field) {
            return false;
        }

        @Override
        public boolean isIndexed(FieldName field) {
            return false;
        }

        @Override
        public boolean hasDocValues(FieldName field) {
            return false;
        }

        @Override
        public boolean hasExactSubfield(FieldName field) {
            return false;
        }

        @Override
        public long count() {
            return 0;
        }

        @Override
        public long count(FieldName field) {
            return 0;
        }

        @Override
        public long count(FieldName field, BytesRef value) {
            return 0;
        }

        @Override
        public byte[] min(FieldName field, DataType dataType) {
            return null;
        }

        @Override
        public byte[] max(FieldName field, DataType dataType) {
            return null;
        }

        @Override
        public boolean isSingleValue(FieldName field) {
            return true;
        }

        @Override
        public boolean canUseEqualityOnSyntheticSourceDelegate(FieldName name, String value) {
            return false;
        }

        @Override
        public boolean matchQueryYieldsCandidateMatchesForEquality(String name) {
            return false;
        }
    }
}
