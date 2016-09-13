/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.permission;

import dk.brics.automaton.Automaton;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.AllFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.xpack.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.security.support.Automatons;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Stores patterns to fields which access is granted or denied to and maintains an automaton that can be used to check if permission is
 * allowed for a specific field.
 * Field permissions are configured via a list of strings that are patterns a field has to match. Two lists determine whether or not a
 * field is granted access to:
 * 1. It has to match the patterns in grantedFieldsArray
 * 2. it must not match the patterns in deniedFieldsArray
 */
public class FieldPermissions implements Writeable, ToXContent {

    // the patterns for fields which we allow access to. if gratedFieldsArray is null we assume that all fields are grated access to
    String[] grantedFieldsArray;
    // the patterns for fields which we deny access to. if this is an empty list or null we assume that we do not deny access to any
    // field explicitly
    String[] deniedFieldsArray;
    // an automaton that matches all strings that match the patterns in permittedFieldsArray but does not match those that also match a
    // pattern in deniedFieldsArray. If permittedFieldsAutomaton is null we assume that all fields are granted access to.
    Automaton permittedFieldsAutomaton;

    // we cannot easily determine if all fields are allowed and we can therefore also allow access to the _all field hence we deny access
    // to _all unless this was explicitly configured.
    boolean allFieldIsAllowed = false;

    public FieldPermissions(StreamInput in) throws IOException {
        this(in.readOptionalStringArray(), in.readOptionalStringArray());
    }

    public FieldPermissions(@Nullable String[] grantedFieldsArray, @Nullable String[] deniedFieldsArray) {
        this.grantedFieldsArray = grantedFieldsArray;
        this.deniedFieldsArray = deniedFieldsArray;
        permittedFieldsAutomaton = initializePermittedFieldsAutomaton(grantedFieldsArray, deniedFieldsArray);
        allFieldIsAllowed = checkAllFieldIsAllowed(grantedFieldsArray, deniedFieldsArray);
    }

    private static boolean checkAllFieldIsAllowed(String[] grantedFieldsArray, String[] deniedFieldsArray) {
        if (deniedFieldsArray != null) {
            for (String fieldName : deniedFieldsArray) {
                if (fieldName.equals(AllFieldMapper.NAME)) {
                   return false;
                }
            }
        }
        if (grantedFieldsArray != null) {
            for (String fieldName : grantedFieldsArray) {
                if (fieldName.equals(AllFieldMapper.NAME)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static Automaton initializePermittedFieldsAutomaton(final String[] grantedFieldsArray,
                                                                final String[] deniedFieldsArray) {
        Automaton grantedFieldsAutomaton;
        if (grantedFieldsArray == null || containsWildcard(grantedFieldsArray)) {
            grantedFieldsAutomaton = Automatons.MATCH_ALL;
        } else {
            grantedFieldsAutomaton = Automatons.patterns(grantedFieldsArray);
        }
        Automaton deniedFieldsAutomaton;
        if (deniedFieldsArray == null || deniedFieldsArray.length == 0) {
            deniedFieldsAutomaton = Automatons.EMPTY;
        } else {
            deniedFieldsAutomaton = Automatons.patterns(deniedFieldsArray);
        }
        if (deniedFieldsAutomaton.subsetOf(grantedFieldsAutomaton) == false) {
            throw new ElasticsearchSecurityException("Exceptions for field permissions must be a subset of the " +
                    "granted fields but " + Arrays.toString(deniedFieldsArray) + " is not a subset of " +
                    Arrays.toString(grantedFieldsArray));
        }

        grantedFieldsAutomaton = grantedFieldsAutomaton.minus(deniedFieldsAutomaton);
        return grantedFieldsAutomaton;
    }

    private static boolean containsWildcard(String[] grantedFieldsArray) {
        for (String fieldPattern : grantedFieldsArray) {
            if (Regex.isMatchAllPattern(fieldPattern)) {
                return true;
            }
        }
        return false;
    }

    public FieldPermissions() {
        this(null, null);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalStringArray(grantedFieldsArray);
        out.writeOptionalStringArray(deniedFieldsArray);
    }

    @Nullable
    String[] getGrantedFieldsArray() {
        return grantedFieldsArray;
    }

    @Nullable
    String[] getDeniedFieldsArray() {
        return deniedFieldsArray;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (grantedFieldsArray != null || deniedFieldsArray != null) {
            sb.append(RoleDescriptor.Fields.FIELD_PERMISSIONS).append("=[");
            if (grantedFieldsArray == null) {
                sb.append(RoleDescriptor.Fields.GRANT_FIELDS).append("=null");
            } else {
                sb.append(RoleDescriptor.Fields.GRANT_FIELDS).append("=[")
                        .append(Strings.arrayToCommaDelimitedString(grantedFieldsArray));
                sb.append("]");
            }
            if (deniedFieldsArray == null) {
                sb.append(", ").append(RoleDescriptor.Fields.EXCEPT_FIELDS).append("=null");
            } else {
                sb.append(", ").append(RoleDescriptor.Fields.EXCEPT_FIELDS).append("=[")
                        .append(Strings.arrayToCommaDelimitedString(deniedFieldsArray));
                sb.append("]");
            }
            sb.append("]");
        }
        return sb.toString();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (grantedFieldsArray != null || deniedFieldsArray != null) {
            builder.startObject(RoleDescriptor.Fields.FIELD_PERMISSIONS.getPreferredName());
            if (grantedFieldsArray != null) {
                builder.array(RoleDescriptor.Fields.GRANT_FIELDS.getPreferredName(), grantedFieldsArray);
            }
            if (deniedFieldsArray != null) {
                builder.array(RoleDescriptor.Fields.EXCEPT_FIELDS.getPreferredName(), deniedFieldsArray);
            }
            builder.endObject();
        }
        return builder;
    }

    /**
     * Returns true if this field permission policy allows access to the field and false if not.
     * fieldName can be a wildcard.
     */
    public boolean grantsAccessTo(String fieldName) {
        if (permittedFieldsAutomaton.isTotal()) {
            return true;
        } else {
            return permittedFieldsAutomaton.run(fieldName);
        }
    }

    // Also, if one grants no access to fields and the other grants all access, merging should result in all access...
    public static FieldPermissions merge(FieldPermissions p1, FieldPermissions p2) {
        Automaton mergedPermittedFieldsAutomaton;
        // we only allow the union of the two automatons
        mergedPermittedFieldsAutomaton = p1.permittedFieldsAutomaton.union(p2.permittedFieldsAutomaton);
        // need to minimize otherwise isTotal() might return false even if one of the merged ones returned true before
        mergedPermittedFieldsAutomaton.minimize();
        // if one of them allows access to _all we allow it for the merged too
        boolean allFieldIsAllowedInMerged = p1.allFieldIsAllowed || p2.allFieldIsAllowed;
        return new MergedFieldPermissions(mergedPermittedFieldsAutomaton, allFieldIsAllowedInMerged);
    }

    public boolean hasFieldLevelSecurity() {
        return permittedFieldsAutomaton.isTotal() == false;
    }

    public Set<String> resolveAllowedFields(Set<String> allowedMetaFields, MapperService mapperService) {
        HashSet<String> finalAllowedFields = new HashSet<>();
        // we always add the allowed meta fields because we must make sure access is not denied accidentally
        finalAllowedFields.addAll(allowedMetaFields);
        // now check all other fields if we allow them
        Collection<String> allFields = mapperService.simpleMatchToIndexNames("*");
        for (String fieldName : allFields) {
            if (grantsAccessTo(fieldName)) {
                finalAllowedFields.add(fieldName);
            }
        }
        if (allFieldIsAllowed == false) {
            // we probably added the _all field and now we have to remove it again
            finalAllowedFields.remove("_all");
        }
        return finalAllowedFields;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FieldPermissions that = (FieldPermissions) o;

        if (allFieldIsAllowed != that.allFieldIsAllowed) return false;
        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        if (!Arrays.equals(grantedFieldsArray, that.grantedFieldsArray)) return false;
        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        if (!Arrays.equals(deniedFieldsArray, that.deniedFieldsArray)) return false;
        return permittedFieldsAutomaton.equals(that.permittedFieldsAutomaton);

    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(grantedFieldsArray);
        result = 31 * result + Arrays.hashCode(deniedFieldsArray);
        result = 31 * result + permittedFieldsAutomaton.hashCode();
        result = 31 * result + (allFieldIsAllowed ? 1 : 0);
        return result;
    }

    /**
     * When we merge field permissions we need to union all the allowed fields. We do this by a union of the automatons
     * that define which fields are granted access too. However, that means that after merging we cannot know anymore
     * which strings defined the automatons. Hence we make a new class that only has an automaton for the fields that
     * we grant access to and that throws an exception whenever we try to access the original patterns that lead to
     * the automaton.
     */
    public static class MergedFieldPermissions extends FieldPermissions {
        public MergedFieldPermissions(Automaton grantedFields, boolean allFieldIsAllowed) {
            assert grantedFields != null;
            this.permittedFieldsAutomaton = grantedFields;
            this.grantedFieldsArray = null;
            this.deniedFieldsArray = null;
            this.allFieldIsAllowed = allFieldIsAllowed;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            throw new UnsupportedOperationException("Cannot build xcontent for merged field permissions");
        }

        @Override
        public String toString() {
            throw new UnsupportedOperationException("Cannot build string for merged field permissions");
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            throw new UnsupportedOperationException("Cannot stream for merged field permissions");
        }

        @Nullable
        public String[] getGrantedFieldsArray() {
            throw new UnsupportedOperationException("Merged field permissions does not maintain sets");
        }

        @Nullable
        public String[] getDeniedFieldsArray() {
            throw new UnsupportedOperationException("Merged field permissions does not maintain sets");
        }
    }
}
