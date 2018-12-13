/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authz.permission;

import org.elasticsearch.common.Strings;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

/**
 * Represents the definition of a {@link FieldPermissions}. Field permissions are defined as a
 * collections of grant and exclude definitions where the exclude definition must be a subset of
 * the grant definition.
 */
public final class FieldPermissionsDefinition {

    private final Set<FieldGrantExcludeGroup> fieldGrantExcludeGroups;

    public FieldPermissionsDefinition(String[] grant, String[] exclude) {
        this(Collections.singleton(new FieldGrantExcludeGroup(grant, exclude)));
    }

    public FieldPermissionsDefinition(Set<FieldGrantExcludeGroup> fieldGrantExcludeGroups) {
        this.fieldGrantExcludeGroups = Collections.unmodifiableSet(fieldGrantExcludeGroups);
    }

    public Set<FieldGrantExcludeGroup> getFieldGrantExcludeGroups() {
        return fieldGrantExcludeGroups;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FieldPermissionsDefinition that = (FieldPermissionsDefinition) o;

        return fieldGrantExcludeGroups != null ?
                fieldGrantExcludeGroups.equals(that.fieldGrantExcludeGroups) :
                that.fieldGrantExcludeGroups == null;
    }

    @Override
    public int hashCode() {
        return fieldGrantExcludeGroups != null ? fieldGrantExcludeGroups.hashCode() : 0;
    }

    public static final class FieldGrantExcludeGroup {
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

            if (!Arrays.equals(grantedFields, that.grantedFields)) return false;
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
                + "[grant=" + Strings.arrayToCommaDelimitedString(grantedFields)
                + "; exclude=" + Strings.arrayToCommaDelimitedString(excludedFields)
                + "]";
        }
    }
}
