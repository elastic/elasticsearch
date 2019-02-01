/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authz.accesscontrol;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xpack.core.security.authz.IndicesAndAliasesResolverField;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissions;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Encapsulates the field and document permissions per concrete index based on the current request.
 */
public class IndicesAccessControl {

    public static final IndicesAccessControl ALLOW_ALL = new IndicesAccessControl(true, Collections.emptyMap());
    public static final IndicesAccessControl ALLOW_NO_INDICES = new IndicesAccessControl(true,
            Collections.singletonMap(IndicesAndAliasesResolverField.NO_INDEX_PLACEHOLDER,
                    new IndicesAccessControl.IndexAccessControl(true, new FieldPermissions(), null)));
    public static final IndicesAccessControl DENIED = new IndicesAccessControl(false, Collections.emptyMap());

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
        private final FieldPermissions fieldPermissions;
        private final Set<BytesReference> queries;

        public IndexAccessControl(boolean granted, FieldPermissions fieldPermissions, Set<BytesReference> queries) {
            this.granted = granted;
            this.fieldPermissions = fieldPermissions;
            this.queries = queries;
        }

        /**
         * @return Whether any role / permission group is allowed to this index.
         */
        public boolean isGranted() {
            return granted;
        }

        /**
         * @return The allowed fields for this index permissions.
         */
        public FieldPermissions getFieldPermissions() {
            return fieldPermissions;
        }

        /**
         * @return The allowed documents expressed as a query for this index permission. If <code>null</code> is returned
         *         then this means that there are no document level restrictions
         */
        @Nullable
        public Set<BytesReference> getQueries() {
            return queries;
        }

        @Override
        public String toString() {
            return "IndexAccessControl{" +
                    "granted=" + granted +
                    ", fieldPermissions=" + fieldPermissions +
                    ", queries=" + queries +
                    '}';
        }
    }

    @Override
    public String toString() {
        return "IndicesAccessControl{" +
                "granted=" + granted +
                ", indexPermissions=" + indexPermissions +
                '}';
    }
}
