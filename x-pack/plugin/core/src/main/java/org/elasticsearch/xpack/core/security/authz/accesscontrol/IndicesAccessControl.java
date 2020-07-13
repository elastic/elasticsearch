/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authz.accesscontrol;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.xpack.core.security.authz.IndicesAndAliasesResolverField;
import org.elasticsearch.xpack.core.security.authz.permission.DocumentPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissions;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Encapsulates the field and document permissions per concrete index based on the current request.
 */
public class IndicesAccessControl {

    public static final IndicesAccessControl ALLOW_ALL = new IndicesAccessControl(true, Map.of());
    public static final IndicesAccessControl ALLOW_NO_INDICES = new IndicesAccessControl(true,
        Map.of(IndicesAndAliasesResolverField.NO_INDEX_PLACEHOLDER,
            new IndicesAccessControl.IndexAccessControl(true, new FieldPermissions(), DocumentPermissions.allowAll())));
    public static final IndicesAccessControl DENIED = new IndicesAccessControl(false, Map.of());

    private final Logger logger = LogManager.getLogger();
    private final boolean granted;
    private Map<String, IndexAccessControl> indexPermissions;

    public IndicesAccessControl(boolean granted, Map<String, IndexAccessControl> indexPermissions) {
        this.granted = granted;
        this.indexPermissions = Map.copyOf(indexPermissions);
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

    public void addPermissionsIfNotPresent(IndicesAccessControl other) {
        if (other.granted != this.granted) {
            throw new IllegalArgumentException("Cannot merge [" + other + "] into [" + this + "]");
        }
        final Map<String, IndexAccessControl> map = new HashMap<>(this.indexPermissions);
        for (Map.Entry<String, IndexAccessControl> entry : other.indexPermissions.entrySet()) {
            String indexName = entry.getKey();
            IndexAccessControl existingControl = map.get(indexName);
            IndexAccessControl newControl = entry.getValue();
            if (existingControl == null) {
                map.put(indexName, newControl);
            } else if (newControl.equals(existingControl) == false) {
                logger.debug("Already have index access control [{}] for [{}], not replacing with [{}]",
                    existingControl, indexName, newControl);
            }
        }
        this.indexPermissions = Map.copyOf(map);
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
            this.fieldPermissions = (fieldPermissions == null) ? FieldPermissions.DEFAULT : fieldPermissions;
            this.documentPermissions = (documentPermissions == null) ? DocumentPermissions.allowAll() : documentPermissions;
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

        /**
         * Returns a instance of {@link IndexAccessControl}, where the privileges for {@code this} object are constrained by the privileges
         * contained in the provided parameter.<br>
         * Allowed fields for this index permission would be an intersection of allowed fields.<br>
         * Allowed documents for this index permission would be an intersection of allowed documents.<br>
         *
         * @param limitedByIndexAccessControl {@link IndexAccessControl}
         * @return {@link IndexAccessControl}
         * @see FieldPermissions#limitFieldPermissions(FieldPermissions)
         * @see DocumentPermissions#limitDocumentPermissions(DocumentPermissions)
         */
        public IndexAccessControl limitIndexAccessControl(IndexAccessControl limitedByIndexAccessControl) {
            final boolean granted;
            if (this.granted == limitedByIndexAccessControl.granted) {
                granted = this.granted;
            } else {
                granted = false;
            }
            FieldPermissions fieldPermissions = getFieldPermissions().limitFieldPermissions(
                    limitedByIndexAccessControl.fieldPermissions);
            DocumentPermissions documentPermissions = getDocumentPermissions()
                    .limitDocumentPermissions(limitedByIndexAccessControl.getDocumentPermissions());
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

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }
            IndexAccessControl that = (IndexAccessControl) other;
            return this.granted == that.granted &&
                this.fieldPermissions.equals(that.fieldPermissions) &&
                this.documentPermissions.equals(that.documentPermissions);
        }

        @Override
        public int hashCode() {
            return Objects.hash(granted, fieldPermissions, documentPermissions);
        }
    }

    /**
     * Returns a instance of {@link IndicesAccessControl}, where the privileges for {@code this}
     * object are constrained by the privileges contained in the provided parameter.<br>
     *
     * @param limitedByIndicesAccessControl {@link IndicesAccessControl}
     * @return {@link IndicesAccessControl}
     */
    public IndicesAccessControl limitIndicesAccessControl(IndicesAccessControl limitedByIndicesAccessControl) {
        final boolean granted;
        if (this.granted == limitedByIndicesAccessControl.granted) {
            granted = this.granted;
        } else {
            granted = false;
        }
        Set<String> indexes = indexPermissions.keySet();
        Set<String> otherIndexes = limitedByIndicesAccessControl.indexPermissions.keySet();
        Set<String> commonIndexes = Sets.intersection(indexes, otherIndexes);

        Map<String, IndexAccessControl> indexPermissions = new HashMap<>(commonIndexes.size());
        for (String index : commonIndexes) {
            IndexAccessControl indexAccessControl = getIndexPermissions(index);
            IndexAccessControl limitedByIndexAccessControl = limitedByIndicesAccessControl.getIndexPermissions(index);
            indexPermissions.put(index, indexAccessControl.limitIndexAccessControl(limitedByIndexAccessControl));
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
