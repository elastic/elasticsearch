/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz.accesscontrol;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;

import java.util.HashSet;
import java.util.Set;

/**
 * Encapsulates the field and document permissions per concrete index based on the current request.
 */
public class IndicesAccessControl {

    public static final IndicesAccessControl ALLOW_ALL = new IndicesAccessControl(true, ImmutableMap.<String, IndexAccessControl>of());

    private final boolean granted;
    private final ImmutableMap<String, IndexAccessControl> indexPermissions;

    public IndicesAccessControl(boolean granted, ImmutableMap<String, IndexAccessControl> indexPermissions) {
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
        private final ImmutableSet<String> fields;
        private final ImmutableSet<BytesReference> queries;

        public IndexAccessControl(boolean granted, ImmutableSet<String> fields, ImmutableSet<BytesReference> queries) {
            this.granted = granted;
            this.fields = fields;
            this.queries = queries;
        }

        /**
         * @return Whether  any role / permission group is allowed to this index.
         */
        public boolean isGranted() {
            return granted;
        }

        /**
         * @return The allowed fields for this index permissions. If <code>null</code> is returned then
         *         this means that there are no field level restrictions
         */
        @Nullable
        public ImmutableSet<String> getFields() {
            return fields;
        }

        /**
         * @return The allowed documents expressed as a query for this index permission. If <code>null</code> is returned
         *         then this means that there are no document level restrictions
         */
        @Nullable
        public ImmutableSet<BytesReference> getQueries() {
            return queries;
        }

        public IndexAccessControl merge(IndexAccessControl other) {
            boolean granted = this.granted;
            if (!granted) {
                granted = other.isGranted();
            }
            // this code is a bit of a pita, but right now we can't just initialize an empty set,
            // because an empty Set means no permissions on fields and
            // <code>null</code> means no field level security
            ImmutableSet<String> fields = null;
            if (this.fields != null || other.getFields() != null) {
                Set<String> _fields = new HashSet<>();
                if (this.fields != null) {
                    _fields.addAll(this.fields);
                }
                if (other.getFields() != null) {
                    _fields.addAll(other.getFields());
                }
                fields = ImmutableSet.copyOf(_fields);
            }
            ImmutableSet<BytesReference> queries = null;
            if (this.queries != null || other.getQueries() != null) {
                Set<BytesReference> _queries = new HashSet<>();
                if (this.queries != null) {
                    _queries.addAll(this.queries);
                }
                if (other.getQueries() != null) {
                    _queries.addAll(other.getQueries());
                }
                queries = ImmutableSet.copyOf(_queries);
            }
            return new IndexAccessControl(granted, fields, queries);
        }

    }

}
