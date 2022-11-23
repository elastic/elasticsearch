/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authz.permission;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.security.authz.support.SecurityQueryTemplateEvaluator.DlsQueryEvaluationContext;
import org.elasticsearch.xpack.core.security.support.CacheKey;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Represents the definition of a {@link FieldPermissions}. Field permissions are defined as a
 * collections of grant and exclude definitions where the exclude definition must be a subset of
 * the grant definition.
 */
public final class FieldPermissionsDefinition implements CacheKey {

    // SortedSet because orders are important when building the request cacheKey
    private final SortedSet<FieldGrantExcludeGroup> fieldGrantExcludeGroups;

    public FieldPermissionsDefinition(String[] grant, String[] exclude) {
        this(Collections.singleton(new FieldGrantExcludeGroup(grant, exclude)));
    }

    public FieldPermissionsDefinition(Set<FieldGrantExcludeGroup> fieldGrantExcludeGroups) {
        this.fieldGrantExcludeGroups = new TreeSet<>(fieldGrantExcludeGroups);
    }

    public Set<FieldGrantExcludeGroup> getFieldGrantExcludeGroups() {
        return Set.copyOf(fieldGrantExcludeGroups);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FieldPermissionsDefinition that = (FieldPermissionsDefinition) o;

        return Objects.equals(fieldGrantExcludeGroups, that.fieldGrantExcludeGroups);
    }

    @Override
    public int hashCode() {
        return fieldGrantExcludeGroups.hashCode();
    }

    @Override
    public String toString() {
        return "FieldPermissionsDefinition{" + "fieldGrantExcludeGroups=" + fieldGrantExcludeGroups + '}';
    }

    @Override
    public void buildCacheKey(StreamOutput out, DlsQueryEvaluationContext context) throws IOException {
        out.writeCollection(fieldGrantExcludeGroups, (o, g) -> g.buildCacheKey(o, context));
    }

    public static final class FieldGrantExcludeGroup implements CacheKey, Comparable<FieldGrantExcludeGroup> {
        private final String[] grantedFields;
        private final String[] excludedFields;

        public FieldGrantExcludeGroup(String[] grantedFields, String[] excludedFields) {
            this.grantedFields = grantedFields;
            this.excludedFields = excludedFields;
        }

        public String[] getGrantedFields() {
            return grantedFields;
        }

        public String[] getExcludedFields() {
            return excludedFields;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            FieldGrantExcludeGroup that = (FieldGrantExcludeGroup) o;

            if (Arrays.equals(grantedFields, that.grantedFields) == false) return false;
            return Arrays.equals(excludedFields, that.excludedFields);
        }

        @Override
        public int hashCode() {
            int result = Arrays.hashCode(grantedFields);
            result = 31 * result + Arrays.hashCode(excludedFields);
            return result;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName()
                + "[grant="
                + Strings.arrayToCommaDelimitedString(grantedFields)
                + "; exclude="
                + Strings.arrayToCommaDelimitedString(excludedFields)
                + "]";
        }

        @Override
        public void buildCacheKey(StreamOutput out, DlsQueryEvaluationContext context) throws IOException {
            out.writeOptionalStringArray(grantedFields);
            out.writeOptionalStringArray(excludedFields);
        }

        @Override
        public int compareTo(FieldGrantExcludeGroup o) {
            final int compare = Arrays.compare(grantedFields, o.grantedFields);
            return compare == 0 ? Arrays.compare(excludedFields, o.excludedFields) : compare;
        }
    }
}
