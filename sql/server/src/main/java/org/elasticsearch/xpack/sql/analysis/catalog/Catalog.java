/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.analysis.catalog;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;

import java.util.Objects;

/**
 * Converts from Elasticsearch's internal metadata ({@link ClusterState})
 * into a representation that is compatible with SQL (@{link {@link EsIndex}).
 */
public interface Catalog {
    /**
     * Lookup the information for a table.
     */
    @Nullable
    GetIndexResult getIndex(String index);

    class GetIndexResult {
        public static GetIndexResult valid(EsIndex index) {
            Objects.requireNonNull(index, "index must not be null if it was found");
            return new GetIndexResult(index, null);
        }
        public static GetIndexResult invalid(String invalid) {
            Objects.requireNonNull(invalid, "invalid must not be null to signal that the index is invalid");
            return new GetIndexResult(null, invalid);
        }
        public static GetIndexResult notFound(String name) {
            Objects.requireNonNull(name, "name must not be null");
            return invalid("Index '" + name + "' does not exist");
        }

        @Nullable
        private final EsIndex index;
        @Nullable
        private final String invalid;

        private GetIndexResult(@Nullable EsIndex index, @Nullable String invalid) {
            this.index = index;
            this.invalid = invalid;
        }

        /**
         * Get the {@linkplain EsIndex} built by the {@linkplain Catalog}.
         * @throws SqlIllegalArgumentException if the index is invalid for
         *      use with sql
         */
        public EsIndex get() {
            if (invalid != null) {
                throw new SqlIllegalArgumentException(invalid);
            }
            return index;
        }

        /**
         * Is the index valid for use with sql? Returns {@code false} if the
         * index wasn't found.
         */
        public boolean isValid() {
            return invalid == null;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || obj.getClass() != getClass()) {
                return false;
            }
            GetIndexResult other = (GetIndexResult) obj;
            return Objects.equals(index, other.index)
                    && Objects.equals(invalid, other.invalid);
        }

        @Override
        public int hashCode() {
            return Objects.hash(index, invalid);
        }

        @Override
        public String toString() {
            if (invalid != null) {
                return "GetIndexResult[" + invalid + "]";
            }
            return "GetIndexResult[" + index + "]";
        }
    }
}
