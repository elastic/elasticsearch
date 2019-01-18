/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authz.accesscontrol;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.xpack.core.security.authz.IndicesAndAliasesResolverField;
import org.elasticsearch.xpack.core.security.authz.permission.DocumentPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissions;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Encapsulates the field and document permissions per concrete index based on the current request.
 */
public class IndicesAccessControl {

    public static final IndicesAccessControl ALLOW_ALL = new IndicesAccessControl(true, Collections.emptyMap());
    public static final IndicesAccessControl ALLOW_NO_INDICES = new IndicesAccessControl(true,
            Collections.singletonMap(IndicesAndAliasesResolverField.NO_INDEX_PLACEHOLDER,
                    new IndicesAccessControl.IndexAccessControl(true, new FieldPermissions(), DocumentPermissions.allowAll())));

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
        private final DocumentPermissions documentPermissions;

        public IndexAccessControl(boolean granted, FieldPermissions fieldPermissions, DocumentPermissions documentPermissions) {
            this.granted = granted;
            this.fieldPermissions = fieldPermissions;
            this.documentPermissions = documentPermissions;
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
        public DocumentPermissions getDocumentPermissions() {
            return documentPermissions;
        }

        public static IndexAccessControl scopedIndexAccessControl(IndexAccessControl indexAccessControl,
                IndexAccessControl scopedByIndexAccessControl) {
            final boolean granted;
            if (indexAccessControl.granted == indexAccessControl.granted) {
                granted = indexAccessControl.granted;
            } else {
                granted = false;
            }
            FieldPermissions fieldPermissions = FieldPermissions.scopedFieldPermissions(indexAccessControl.fieldPermissions,
                    scopedByIndexAccessControl.fieldPermissions);
            DocumentPermissions documentPermissions = DocumentPermissions.scopedDocumentPermissions(
                    indexAccessControl.getDocumentPermissions(), scopedByIndexAccessControl.getDocumentPermissions());
            return new IndexAccessControl(granted, fieldPermissions, documentPermissions);
        }

        @Override
        public String toString() {
            return "IndexAccessControl{" +
                    "granted=" + granted +
                    ", fieldPermissions=" + fieldPermissions +
                    ", documentPermissions=" + documentPermissions +
                    '}';
        }
    }

    public static IndicesAccessControl scopedIndicesAccessControl(IndicesAccessControl indicesAccessControl,
            IndicesAccessControl scopedByIndicesAccessControl) {
        final boolean granted;
        if (indicesAccessControl.granted == scopedByIndicesAccessControl.granted) {
            granted = indicesAccessControl.granted;
        } else {
            granted = false;
        }
        Set<String> indexes = indicesAccessControl.indexPermissions.keySet();
        Set<String> otherIndexes = scopedByIndicesAccessControl.indexPermissions.keySet();
        Set<String> commonIndexes = Sets.intersection(indexes, otherIndexes);

        Map<String, IndexAccessControl> indexPermissions = new HashMap<>(commonIndexes.size());
        for (String index : commonIndexes) {
            IndexAccessControl indexAccessControl = indicesAccessControl.getIndexPermissions(index);
            IndexAccessControl otherIndexAccessControl = scopedByIndicesAccessControl.getIndexPermissions(index);
            indexPermissions.put(index, IndexAccessControl.scopedIndexAccessControl(indexAccessControl, otherIndexAccessControl));
        }
        return new IndicesAccessControl(granted, indexPermissions);
    }

    @Override
    public String toString() {
        return "IndicesAccessControl{" +
                "granted=" + granted +
                ", indexPermissions=" + indexPermissions +
                '}';
    }
}
