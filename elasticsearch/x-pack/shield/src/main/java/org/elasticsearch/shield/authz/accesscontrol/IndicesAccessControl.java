/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz.accesscontrol;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.unmodifiableSet;

/**
 * Encapsulates the field and document permissions per concrete index based on the current request.
 */
public class IndicesAccessControl {

    public static final IndicesAccessControl ALLOW_ALL = new IndicesAccessControl(true, Collections.emptyMap());

    private final boolean granted;
    private final Map<String, IndexAccessControl> indexPermissions;

    public IndicesAccessControl(boolean granted, Map<String, IndexAccessControl> indexPermissions) {
        this.granted = granted;
        this.indexPermissions = indexPermissions;
    }

    /**
     * @return The document and field permissions for an index if exist, otherwise <code>null</code> is returned.
     *         If <code>null</code> is being returned this means that there are no field or document level restrictions.
     */
    @Nullable
    public IndexAccessControl getIndexPermissions(String index) {
        return indexPermissions.get(index);
    }

    /**
     * @return Whether any role / permission group is allowed to access all indices.
     */
    public boolean isGranted() {
        return granted;
    }

    /**
     * Encapsulates the field and document permissions for an index.
     */
    public static class IndexAccessControl {

        private final boolean granted;
        private final Set<String> fields;
        private final Set<BytesReference> queries;

        public IndexAccessControl(boolean granted, Set<String> fields, Set<BytesReference> queries) {
            this.granted = granted;
            this.fields = fields;
            this.queries = queries;
        }

        /**
         * @return Whether any role / permission group is allowed to this index.
         */
        public boolean isGranted() {
            return granted;
        }

        /**
         * @return The allowed fields for this index permissions. If <code>null</code> is returned then
         *         this means that there are no field level restrictions
         */
        @Nullable
        public Set<String> getFields() {
            return fields;
        }

        /**
         * @return The allowed documents expressed as a query for this index permission. If <code>null</code> is returned
         *         then this means that there are no document level restrictions
         */
        @Nullable
        public Set<BytesReference> getQueries() {
            return queries;
        }

        public IndexAccessControl merge(IndexAccessControl other) {
            if (other.isGranted() == false) {
                // nothing to merge
                return this;
            }

            final boolean granted = this.granted;
            if (granted == false) {
                // we do not support negatives, so if the current isn't granted - just return other
                assert other.isGranted();
                return other;
            }

            // this code is a bit of a pita, but right now we can't just initialize an empty set,
            // because an empty Set means no permissions on fields and
            // <code>null</code> means no field level security
            // Also, if one grants no access to fields and the other grants all access, merging should result in all access...
            Set<String> fields = null;
            if (this.fields != null && other.getFields() != null) {
                fields = new HashSet<>();
                if (this.fields != null) {
                    fields.addAll(this.fields);
                }
                if (other.getFields() != null) {
                    fields.addAll(other.getFields());
                }
                fields = unmodifiableSet(fields);
            }
            Set<BytesReference> queries = null;
            if (this.queries != null && other.getQueries() != null) {
                queries = new HashSet<>();
                if (this.queries != null) {
                    queries.addAll(this.queries);
                }
                if (other.getQueries() != null) {
                    queries.addAll(other.getQueries());
                }
                queries = unmodifiableSet(queries);
            }
            return new IndexAccessControl(granted, fields, queries);
        }

    }

}
