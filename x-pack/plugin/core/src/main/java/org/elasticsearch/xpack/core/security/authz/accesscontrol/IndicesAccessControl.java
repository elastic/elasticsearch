/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authz.accesscontrol;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.security.authz.IndicesAndAliasesResolverField;
import org.elasticsearch.xpack.core.security.authz.permission.DocumentPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissions;
import org.elasticsearch.xpack.core.security.authz.support.SecurityQueryTemplateEvaluator.DlsQueryEvaluationContext;
import org.elasticsearch.xpack.core.security.support.CacheKey;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Encapsulates the field and document permissions per concrete index based on the current request.
 */
public class IndicesAccessControl {

    public static final IndicesAccessControl ALLOW_NO_INDICES = new IndicesAccessControl(
        true,
        Collections.singletonMap(
            IndicesAndAliasesResolverField.NO_INDEX_PLACEHOLDER,
            new IndicesAccessControl.IndexAccessControl(true, new FieldPermissions(), DocumentPermissions.allowAll())
        )
    );
    public static final IndicesAccessControl DENIED = new IndicesAccessControl(false, Collections.emptyMap());

    private final boolean granted;
    private final Map<String, IndexAccessControl> indexPermissions;

    public IndicesAccessControl(boolean granted, Map<String, IndexAccessControl> indexPermissions) {
        this.granted = granted;
        this.indexPermissions = Objects.requireNonNull(indexPermissions);
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

    public Collection<String> getDeniedIndices() {
        return this.indexPermissions.entrySet()
            .stream()
            .filter(e -> e.getValue().granted == false)
            .map(Map.Entry::getKey)
            .collect(Collectors.toUnmodifiableSet());
    }

    public DlsFlsUsage getFieldAndDocumentLevelSecurityUsage() {
        boolean hasFls = false;
        boolean hasDls = false;
        for (IndexAccessControl iac : indexPermissions.values()) {
            if (iac.fieldPermissions.hasFieldLevelSecurity()) {
                hasFls = true;
            }
            if (iac.documentPermissions.hasDocumentLevelPermissions()) {
                hasDls = true;
            }
            if (hasFls && hasDls) {
                return DlsFlsUsage.BOTH;
            }
        }
        if (hasFls) {
            return DlsFlsUsage.FLS;
        } else if (hasDls) {
            return DlsFlsUsage.DLS;
        } else {
            return DlsFlsUsage.NONE;
        }
    }

    public List<String> getIndicesWithFieldOrDocumentLevelSecurity() {
        return getIndexNames(iac -> iac.fieldPermissions.hasFieldLevelSecurity() || iac.documentPermissions.hasDocumentLevelPermissions());
    }

    public List<String> getIndicesWithFieldLevelSecurity() {
        return getIndexNames(iac -> iac.fieldPermissions.hasFieldLevelSecurity());
    }

    public List<String> getIndicesWithDocumentLevelSecurity() {
        return getIndexNames(iac -> iac.documentPermissions.hasDocumentLevelPermissions());
    }

    private List<String> getIndexNames(Predicate<IndexAccessControl> predicate) {
        return indexPermissions.entrySet().stream().filter(entry -> predicate.test(entry.getValue())).map(Map.Entry::getKey).toList();
    }

    public enum DlsFlsUsage {
        NONE,

        DLS() {
            @Override
            public boolean hasDocumentLevelSecurity() {
                return true;
            }
        },

        FLS() {
            @Override
            public boolean hasFieldLevelSecurity() {
                return true;
            }
        },

        BOTH() {
            @Override
            public boolean hasFieldLevelSecurity() {
                return true;
            }

            @Override
            public boolean hasDocumentLevelSecurity() {
                return true;
            }
        };

        public boolean hasFieldLevelSecurity() {
            return false;
        }

        public boolean hasDocumentLevelSecurity() {
            return false;
        }
    }

    /**
     * Encapsulates the field and document permissions for an index.
     */
    public static class IndexAccessControl implements CacheKey {

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
         * Returns an instance of {@link IndexAccessControl}, where the privileges for {@code this} object are constrained by the privileges
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
            final boolean isGranted;
            if (this.granted == limitedByIndexAccessControl.granted) {
                isGranted = this.granted;
            } else {
                isGranted = false;
            }
            FieldPermissions constrainedFieldPermissions = getFieldPermissions().limitFieldPermissions(
                limitedByIndexAccessControl.fieldPermissions
            );
            DocumentPermissions constrainedDocumentPermissions = getDocumentPermissions().limitDocumentPermissions(
                limitedByIndexAccessControl.getDocumentPermissions()
            );
            return new IndexAccessControl(isGranted, constrainedFieldPermissions, constrainedDocumentPermissions);
        }

        @Override
        public String toString() {
            return "IndexAccessControl{"
                + "granted="
                + granted
                + ", fieldPermissions="
                + fieldPermissions
                + ", documentPermissions="
                + documentPermissions
                + '}';
        }

        @Override
        public void buildCacheKey(StreamOutput out, DlsQueryEvaluationContext context) throws IOException {
            if (documentPermissions.hasDocumentLevelPermissions()) {
                out.writeBoolean(true);
                documentPermissions.buildCacheKey(out, context);
            } else {
                out.writeBoolean(false);
            }
            if (fieldPermissions.hasFieldLevelSecurity()) {
                out.writeBoolean(true);
                fieldPermissions.buildCacheKey(out, context);
            } else {
                out.writeBoolean(false);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            IndexAccessControl that = (IndexAccessControl) o;
            return granted == that.granted
                && Objects.equals(fieldPermissions, that.fieldPermissions)
                && Objects.equals(documentPermissions, that.documentPermissions);
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
        if (this instanceof AllowAllIndicesAccessControl) {
            return limitedByIndicesAccessControl;
        } else if (limitedByIndicesAccessControl instanceof AllowAllIndicesAccessControl) {
            return this;
        }

        final boolean isGranted;
        if (this.granted == limitedByIndicesAccessControl.granted) {
            isGranted = this.granted;
        } else {
            isGranted = false;
        }
        Set<String> indexes = indexPermissions.keySet();
        Set<String> otherIndexes = limitedByIndicesAccessControl.indexPermissions.keySet();
        Set<String> commonIndexes = Sets.intersection(indexes, otherIndexes);

        Map<String, IndexAccessControl> indexPermissionsMap = Maps.newMapWithExpectedSize(commonIndexes.size());
        for (String index : commonIndexes) {
            IndexAccessControl indexAccessControl = getIndexPermissions(index);
            IndexAccessControl limitedByIndexAccessControl = limitedByIndicesAccessControl.getIndexPermissions(index);
            indexPermissionsMap.put(index, indexAccessControl.limitIndexAccessControl(limitedByIndexAccessControl));
        }
        return new IndicesAccessControl(isGranted, indexPermissionsMap);
    }

    @Override
    public String toString() {
        return "IndicesAccessControl{" + "granted=" + granted + ", indexPermissions=" + indexPermissions + '}';
    }

    public static IndicesAccessControl allowAll() {
        return AllowAllIndicesAccessControl.INSTANCE;
    }

    private static class AllowAllIndicesAccessControl extends IndicesAccessControl {

        private static final IndicesAccessControl INSTANCE = new AllowAllIndicesAccessControl();

        private final IndexAccessControl allowAllIndexAccessControl = new IndexAccessControl(true, null, null);

        private AllowAllIndicesAccessControl() {
            super(true, Map.of());
        }

        @Override
        public IndexAccessControl getIndexPermissions(String index) {
            return allowAllIndexAccessControl;
        }

        @Override
        public String toString() {
            return "AllowAllIndicesAccessControl{}";
        }
    }

}
