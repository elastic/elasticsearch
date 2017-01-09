/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.permission;

import org.apache.lucene.util.automaton.Automaton;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.mapper.AllFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.xpack.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.security.support.Automatons;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.apache.lucene.util.automaton.Operations.isTotal;
import static org.apache.lucene.util.automaton.Operations.run;
import static org.apache.lucene.util.automaton.Operations.subsetOf;
import static org.elasticsearch.xpack.security.support.Automatons.minusAndMinimize;

/**
 * Stores patterns to fields which access is granted or denied to and maintains an automaton that can be used to check if permission is
 * allowed for a specific field.
 * Field permissions are configured via a list of strings that are patterns a field has to match. Two lists determine whether or not a
 * field is granted access to:
 * 1. It has to match the patterns in grantedFieldsArray
 * 2. it must not match the patterns in deniedFieldsArray
 */
public final class FieldPermissions implements Writeable {

    public static final FieldPermissions DEFAULT = new FieldPermissions();

    // the patterns for fields which we allow access to. if gratedFieldsArray is null we assume that all fields are grated access to
    private final String[] grantedFieldsArray;
    // the patterns for fields which we deny access to. if this is an empty list or null we assume that we do not deny access to any
    // field explicitly
    private final String[] deniedFieldsArray;
    // an automaton that matches all strings that match the patterns in permittedFieldsArray but does not match those that also match a
    // pattern in deniedFieldsArray. If permittedFieldsAutomaton is null we assume that all fields are granted access to.
    private final Automaton permittedFieldsAutomaton;

    // we cannot easily determine if all fields are allowed and we can therefore also allow access to the _all field hence we deny access
    // to _all unless this was explicitly configured.
    private final boolean allFieldIsAllowed;

    public FieldPermissions() {
        this(null, null);
    }

    public FieldPermissions(StreamInput in) throws IOException {
        this(in.readOptionalStringArray(), in.readOptionalStringArray());
    }

    public FieldPermissions(@Nullable String[] grantedFieldsArray, @Nullable String[] deniedFieldsArray) {
        this(grantedFieldsArray, deniedFieldsArray, initializePermittedFieldsAutomaton(grantedFieldsArray, deniedFieldsArray),
                checkAllFieldIsAllowed(grantedFieldsArray, deniedFieldsArray));
    }

    FieldPermissions(@Nullable String[] grantedFieldsArray, @Nullable String[] deniedFieldsArray,
                             Automaton permittedFieldsAutomaton, boolean allFieldIsAllowed) {
        this.grantedFieldsArray = grantedFieldsArray;
        this.deniedFieldsArray = deniedFieldsArray;
        this.permittedFieldsAutomaton = permittedFieldsAutomaton;
        this.allFieldIsAllowed = allFieldIsAllowed;
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

    private static Automaton initializePermittedFieldsAutomaton(final String[] grantedFieldsArray, final String[] deniedFieldsArray) {
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
        if (subsetOf(deniedFieldsAutomaton, grantedFieldsAutomaton) == false) {
            throw new ElasticsearchSecurityException("Exceptions for field permissions must be a subset of the " +
                    "granted fields but " + Arrays.toString(deniedFieldsArray) + " is not a subset of " +
                    Arrays.toString(grantedFieldsArray));
        }

        grantedFieldsAutomaton = minusAndMinimize(grantedFieldsAutomaton, deniedFieldsAutomaton);
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

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalStringArray(grantedFieldsArray);
        out.writeOptionalStringArray(deniedFieldsArray);
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

    /**
     * Returns true if this field permission policy allows access to the field and false if not.
     * fieldName can be a wildcard.
     */
    public boolean grantsAccessTo(String fieldName) {
        return isTotal(permittedFieldsAutomaton) || run(permittedFieldsAutomaton, fieldName);
    }

    Automaton getPermittedFieldsAutomaton() {
        return permittedFieldsAutomaton;
    }

    @Nullable
    String[] getGrantedFieldsArray() {
        return grantedFieldsArray;
    }

    @Nullable
    String[] getDeniedFieldsArray() {
        return deniedFieldsArray;
    }

    public boolean hasFieldLevelSecurity() {
        return isTotal(permittedFieldsAutomaton) == false;
    }

    boolean isAllFieldIsAllowed() {
        return allFieldIsAllowed;
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
        return Arrays.equals(deniedFieldsArray, that.deniedFieldsArray);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(grantedFieldsArray);
        result = 31 * result + Arrays.hashCode(deniedFieldsArray);
        result = 31 * result + (allFieldIsAllowed ? 1 : 0);
        return result;
    }
}
