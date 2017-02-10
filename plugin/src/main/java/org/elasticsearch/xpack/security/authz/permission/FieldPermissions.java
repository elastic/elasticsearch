/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.permission;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.MinimizationOperations;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.mapper.AllFieldMapper;
import org.elasticsearch.xpack.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.security.authz.accesscontrol.FieldSubsetReader;
import org.elasticsearch.xpack.security.support.Automatons;

import java.io.IOException;
import java.util.Arrays;

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
public final class FieldPermissions implements Writeable, Accountable {

    public static final FieldPermissions DEFAULT = new FieldPermissions();

    // the patterns for fields which we allow access to. if gratedFieldsArray is null we assume that all fields are grated access to
    private final String[] grantedFieldsArray;
    // the patterns for fields which we deny access to. if this is an empty list or null we assume that we do not deny access to any
    // field explicitly
    private final String[] deniedFieldsArray;
    // an automaton that matches all strings that match the patterns in permittedFieldsArray but does not match those that also match a
    // pattern in deniedFieldsArray. If permittedFieldsAutomaton is null we assume that all fields are granted access to.
    private final CharacterRunAutomaton permittedFieldsAutomaton;
    private final boolean permittedFieldsAutomatonIsTotal;

    private final long ramBytesUsed;

    /** Constructor that does not enable field-level security: all fields are accepted. */
    public FieldPermissions() {
        this(null, null);
    }

    public FieldPermissions(StreamInput in) throws IOException {
        this(in.readOptionalStringArray(), in.readOptionalStringArray());
    }

    /** Constructor that enables field-level security based on include/exclude rules. Exclude rules
     *  have precedence over include rules. */
    public FieldPermissions(@Nullable String[] grantedFieldsArray, @Nullable String[] deniedFieldsArray) {
        this(grantedFieldsArray, deniedFieldsArray, initializePermittedFieldsAutomaton(grantedFieldsArray, deniedFieldsArray));
    }

    private FieldPermissions(@Nullable String[] grantedFieldsArray, @Nullable String[] deniedFieldsArray,
                             Automaton permittedFieldsAutomaton) {
        if (permittedFieldsAutomaton.isDeterministic() == false && permittedFieldsAutomaton.getNumStates() > 1) {
            // we only accept deterministic automata so that the CharacterRunAutomaton constructor
            // directly wraps the provided automaton
            throw new IllegalArgumentException("Only accepts deterministic automata");
        }
        this.grantedFieldsArray = grantedFieldsArray;
        this.deniedFieldsArray = deniedFieldsArray;
        this.permittedFieldsAutomaton = new CharacterRunAutomaton(permittedFieldsAutomaton);
        // we cache the result of isTotal since this might be a costly operation
        this.permittedFieldsAutomatonIsTotal = Operations.isTotal(permittedFieldsAutomaton);

        long ramBytesUsed = ramBytesUsed(grantedFieldsArray);
        ramBytesUsed += ramBytesUsed(deniedFieldsArray);
        ramBytesUsed += permittedFieldsAutomaton.ramBytesUsed();
        ramBytesUsed += runAutomatonRamBytesUsed(permittedFieldsAutomaton);
        this.ramBytesUsed = ramBytesUsed;
    }

    /**
     * Return an estimation of the ram bytes used by the given {@link String}
     * array.
     */
    private static long ramBytesUsed(String[] array) {
        long ramBytesUsed = 0;
        if (array != null) {
            ramBytesUsed += RamUsageEstimator.shallowSizeOf(array);
            for (String s : array) {
                // might be overestimated because of compact strings but it is better to overestimate here
                ramBytesUsed += s.length() * Character.BYTES;
            }
        }
        return ramBytesUsed;
    }

    /**
     * Return an estimation of the ram bytes used by a {@link CharacterRunAutomaton}
     * that wraps the given automaton.
     */
    private static long runAutomatonRamBytesUsed(Automaton a) {
        return a.getNumStates() * 5; // wild guess, better than 0
    }

    private static Automaton initializePermittedFieldsAutomaton(final String[] grantedFieldsArray, final String[] deniedFieldsArray) {
        Automaton grantedFieldsAutomaton;
        if (grantedFieldsArray == null || Arrays.stream(grantedFieldsArray).anyMatch(Regex::isMatchAllPattern)) {
            grantedFieldsAutomaton = Automatons.MATCH_ALL;
        } else {
            // an automaton that includes metadata fields, including join fields created by the _parent field such
            // as _parent#type
            Automaton metaFieldsAutomaton = Operations.concatenate(Automata.makeChar('_'), Automata.makeAnyString());
            grantedFieldsAutomaton = Operations.union(Automatons.patterns(grantedFieldsArray), metaFieldsAutomaton);
        }

        Automaton deniedFieldsAutomaton;
        if (deniedFieldsArray == null || deniedFieldsArray.length == 0) {
            deniedFieldsAutomaton = Automatons.EMPTY;
        } else {
            deniedFieldsAutomaton = Automatons.patterns(deniedFieldsArray);
        }

        grantedFieldsAutomaton = MinimizationOperations.minimize(grantedFieldsAutomaton, Operations.DEFAULT_MAX_DETERMINIZED_STATES);
        deniedFieldsAutomaton = MinimizationOperations.minimize(deniedFieldsAutomaton, Operations.DEFAULT_MAX_DETERMINIZED_STATES);

        if (subsetOf(deniedFieldsAutomaton, grantedFieldsAutomaton) == false) {
            throw new ElasticsearchSecurityException("Exceptions for field permissions must be a subset of the " +
                    "granted fields but " + Arrays.toString(deniedFieldsArray) + " is not a subset of " +
                    Arrays.toString(grantedFieldsArray));
        }

        if ((grantedFieldsArray == null || Arrays.asList(grantedFieldsArray).contains(AllFieldMapper.NAME) == false) &&
                (deniedFieldsArray == null || Arrays.asList(deniedFieldsArray).contains(AllFieldMapper.NAME) == false)) {
            // It is not explicitly stated whether _all should be allowed
            // In that case we automatically disable _all, unless all fields would match
            if (Operations.isTotal(grantedFieldsAutomaton) && Operations.isEmpty(deniedFieldsAutomaton)) {
                // all fields are accepted, so using _all is fine
            } else {
                deniedFieldsAutomaton = Operations.union(deniedFieldsAutomaton, Automata.makeString(AllFieldMapper.NAME));
            }
        }

        grantedFieldsAutomaton = minusAndMinimize(grantedFieldsAutomaton, deniedFieldsAutomaton);
        return grantedFieldsAutomaton;
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
        return permittedFieldsAutomatonIsTotal || permittedFieldsAutomaton.run(fieldName);
    }

    @Nullable
    String[] getGrantedFieldsArray() {
        return grantedFieldsArray;
    }

    @Nullable
    String[] getDeniedFieldsArray() {
        return deniedFieldsArray;
    }

    /** Return whether field-level security is enabled, ie. whether any field might be filtered out. */
    public boolean hasFieldLevelSecurity() {
        return permittedFieldsAutomatonIsTotal == false;
    }

    /** Return a wrapped reader that only exposes allowed fields. */
    public DirectoryReader filter(DirectoryReader reader) throws IOException {
        if (hasFieldLevelSecurity() == false) {
            return reader;
        }
        return FieldSubsetReader.wrap(reader, permittedFieldsAutomaton);
    }

    // for testing only
    CharacterRunAutomaton getIncludeAutomaton() {
        return permittedFieldsAutomaton;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FieldPermissions that = (FieldPermissions) o;

        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        if (!Arrays.equals(grantedFieldsArray, that.grantedFieldsArray)) return false;
        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        return Arrays.equals(deniedFieldsArray, that.deniedFieldsArray);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(grantedFieldsArray);
        result = 31 * result + Arrays.hashCode(deniedFieldsArray);
        return result;
    }

    @Override
    public long ramBytesUsed() {
        return ramBytesUsed;
    }
}
