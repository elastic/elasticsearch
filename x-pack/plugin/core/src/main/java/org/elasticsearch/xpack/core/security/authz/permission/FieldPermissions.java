/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authz.permission;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.mapper.DataStreamTimestampFieldMapper;
import org.elasticsearch.index.mapper.DocCountFieldMapper;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.IgnoredFieldMapper;
import org.elasticsearch.index.mapper.IgnoredSourceFieldMapper;
import org.elasticsearch.index.mapper.IndexFieldMapper;
import org.elasticsearch.index.mapper.IndexModeFieldMapper;
import org.elasticsearch.index.mapper.NestedPathFieldMapper;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesRoutingHashFieldMapper;
import org.elasticsearch.index.mapper.VersionFieldMapper;
import org.elasticsearch.lucene.util.automaton.MinimizationOperations;
import org.elasticsearch.plugins.FieldPredicate;
import org.elasticsearch.xpack.cluster.routing.allocation.mapper.DataTierFieldMapper;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.FieldSubsetReader;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsDefinition.FieldGrantExcludeGroup;
import org.elasticsearch.xpack.core.security.authz.support.SecurityQueryTemplateEvaluator.DlsQueryEvaluationContext;
import org.elasticsearch.xpack.core.security.support.Automatons;
import org.elasticsearch.xpack.core.security.support.CacheKey;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Stores patterns to fields which access is granted or denied to and maintains an automaton that can be used to check if permission is
 * allowed for a specific field.
 * Field permissions are configured via a list of strings that are patterns a field has to match. Two lists determine whether or
 * not a field is granted access to:
 * 1. It has to match the patterns in grantedFieldsArray
 * 2. it must not match the patterns in deniedFieldsArray
 */
public final class FieldPermissions implements Accountable, CacheKey {
    public static final FieldPermissions DEFAULT = new FieldPermissions();
    // public for testing
    public static final Set<String> METADATA_FIELDS_ALLOWLIST = Set.of(
        // built-in
        IgnoredFieldMapper.NAME,
        IdFieldMapper.NAME,
        RoutingFieldMapper.NAME,
        TimeSeriesIdFieldMapper.NAME,
        TimeSeriesRoutingHashFieldMapper.NAME,
        IndexFieldMapper.NAME,
        IndexModeFieldMapper.NAME,
        SourceFieldMapper.NAME,
        IgnoredSourceFieldMapper.NAME,
        NestedPathFieldMapper.NAME,
        NestedPathFieldMapper.NAME_PRE_V8,
        VersionFieldMapper.NAME,
        SeqNoFieldMapper.NAME,
        SeqNoFieldMapper.PRIMARY_TERM_NAME,
        DocCountFieldMapper.NAME,
        DataStreamTimestampFieldMapper.NAME,
        FieldNamesFieldMapper.NAME,
        // plugins
        // MapperSizePlugin
        "_size",
        // MapperExtrasPlugin
        "_feature",
        // XPackPlugin
        DataTierFieldMapper.NAME
    );
    public static final FieldPredicate PREDICATE = new FieldPredicate() {
        @Override
        public boolean test(String field) {
            return METADATA_FIELDS_ALLOWLIST.contains(field) || (field.startsWith("_") && isMetadataSubField(field));
        }

        /**
         * Matches metadata sub-fields, i.e., {metadata_field}.*
         */
        private boolean isMetadataSubField(String field) {
            for (String f : METADATA_FIELDS_ALLOWLIST) {
                if (field.startsWith(f + ".")) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public String modifyHash(String hash) {
            return hash + ":metadata_fields_allowlist";
        }

        @Override
        public long ramBytesUsed() {
            return 0; // shared
        }

        @Override
        public String toString() {
            return "metadata fields allowlist";
        }
    };

    private static final long BASE_FIELD_PERM_DEF_BYTES = RamUsageEstimator.shallowSizeOf(new FieldPermissionsDefinition(null, null));
    private static final long BASE_FIELD_GROUP_BYTES = RamUsageEstimator.shallowSizeOf(new FieldGrantExcludeGroup(null, null));
    private static final long BASE_HASHSET_ENTRY_SIZE;
    static {
        HashMap<String, Object> map = new HashMap<>();
        map.put(FieldPermissions.class.getName(), new Object());
        long mapEntryShallowSize = RamUsageEstimator.shallowSizeOf(map.entrySet().iterator().next());
        // assume a load factor of 50%
        // for each entry, we need two object refs, one for the entry itself
        // and one for the free space that is due to the fact hash tables can
        // not be fully loaded
        BASE_HASHSET_ENTRY_SIZE = mapEntryShallowSize + 2 * RamUsageEstimator.NUM_BYTES_OBJECT_REF;
    }

    private final List<FieldPermissionsDefinition> fieldPermissionsDefinitions;

    // an automaton that represents a union of one more sets of permitted and denied fields
    private final CharacterRunAutomaton permittedFieldsAutomaton;
    private final boolean permittedFieldsAutomatonIsTotal;
    private final Automaton originalAutomaton;
    private final FieldPredicate fieldPredicate;

    private final long ramBytesUsed;
    private final boolean hasLegacyExceptFields;

    /** Constructor that does not enable field-level security: all fields are accepted. */
    private FieldPermissions() {
        this(new FieldPermissionsDefinition(null, null), new AutomatonWithLegacyExceptFieldsFlag(Automatons.MATCH_ALL, false));
    }

    /** Constructor that enables field-level security based on include/exclude rules. Exclude rules
     *  have precedence over include rules. */
    public FieldPermissions(FieldPermissionsDefinition fieldPermissionsDefinition) {
        this(fieldPermissionsDefinition, initializePermittedFieldsAutomaton(fieldPermissionsDefinition));
    }

    /** Constructor that enables field-level security based on include/exclude rules. Exclude rules
     *  have precedence over include rules. */
    FieldPermissions(
        FieldPermissionsDefinition fieldPermissionsDefinition,
        AutomatonWithLegacyExceptFieldsFlag permittedFieldsAutomatonWithFlag
    ) {
        this(
            List.of(Objects.requireNonNull(fieldPermissionsDefinition, "field permission definition cannot be null")),
            permittedFieldsAutomatonWithFlag
        );
    }

    /** Constructor that enables field-level security based on include/exclude rules. Exclude rules
     *  have precedence over include rules. */
    private FieldPermissions(
        List<FieldPermissionsDefinition> fieldPermissionsDefinitions,
        AutomatonWithLegacyExceptFieldsFlag permittedFieldsAutomatonWithFlag
    ) {
        var permittedFieldsAutomaton = permittedFieldsAutomatonWithFlag.automaton();
        if (permittedFieldsAutomaton.isDeterministic() == false && permittedFieldsAutomaton.getNumStates() > 1) {
            // we only accept deterministic automata so that the CharacterRunAutomaton constructor
            // directly wraps the provided automaton
            throw new IllegalArgumentException("Only accepts deterministic automata");
        }
        this.fieldPermissionsDefinitions = Objects.requireNonNull(
            fieldPermissionsDefinitions,
            "field permission definitions cannot be null"
        );
        this.originalAutomaton = permittedFieldsAutomaton;
        this.permittedFieldsAutomaton = new CharacterRunAutomaton(permittedFieldsAutomaton);
        // we cache the result of isTotal since this might be a costly operation
        this.permittedFieldsAutomatonIsTotal = Operations.isTotal(permittedFieldsAutomaton);
        this.fieldPredicate = permittedFieldsAutomatonIsTotal
            ? FieldPredicate.ACCEPT_ALL
            : new FieldPredicate.Or(PREDICATE, new AutomatonFieldPredicate(originalAutomaton, this.permittedFieldsAutomaton));

        long ramBytesUsed = BASE_FIELD_PERM_DEF_BYTES;
        ramBytesUsed += this.fieldPermissionsDefinitions.stream()
            .mapToLong(FieldPermissions::ramBytesUsedForFieldPermissionsDefinition)
            .sum();
        ramBytesUsed += permittedFieldsAutomaton.ramBytesUsed();
        ramBytesUsed += runAutomatonRamBytesUsed(permittedFieldsAutomaton);
        ramBytesUsed += fieldPredicate.ramBytesUsed();
        ramBytesUsed += 1; // for the `hasLegacyExceptionFields` boolean flag
        this.ramBytesUsed = ramBytesUsed;
        this.hasLegacyExceptFields = permittedFieldsAutomatonWithFlag.hasLegacyExceptFields();
    }

    private static long ramBytesUsedForFieldPermissionsDefinition(FieldPermissionsDefinition fpd) {
        long ramBytesUsed = 0L;
        for (FieldGrantExcludeGroup group : fpd.getFieldGrantExcludeGroups()) {
            ramBytesUsed += BASE_FIELD_GROUP_BYTES + BASE_HASHSET_ENTRY_SIZE;
            if (group.getGrantedFields() != null) {
                ramBytesUsed += RamUsageEstimator.shallowSizeOf(group.getGrantedFields());
            }
            if (group.getExcludedFields() != null) {
                ramBytesUsed += RamUsageEstimator.shallowSizeOf(group.getExcludedFields());
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

    /**
     * Construct a single automaton to represent the set of {@code grantedFields} except for the {@code deniedFields}.
     * @throws ElasticsearchSecurityException If {@code deniedFields} is not a subset of {@code grantedFields}.
     */
    static AutomatonWithLegacyExceptFieldsFlag initializePermittedFieldsAutomaton(FieldPermissionsDefinition fieldPermissionsDefinition) {
        Set<FieldGrantExcludeGroup> groups = fieldPermissionsDefinition.getFieldGrantExcludeGroups();
        assert false == groups.isEmpty() : "there must always be a single group for field inclusion/exclusion";
        boolean hasLegacyExceptionFields = false;
        List<Automaton> automatonList = new ArrayList<>();
        for (FieldGrantExcludeGroup g : groups) {
            AutomatonWithLegacyExceptFieldsFlag automatonWithFlag = FieldPermissions.buildPermittedFieldsAutomaton(
                g.getGrantedFields(),
                g.getExcludedFields()
            );
            automatonList.add(automatonWithFlag.automaton());
            hasLegacyExceptionFields = hasLegacyExceptionFields || automatonWithFlag.hasLegacyExceptFields();
        }
        return new AutomatonWithLegacyExceptFieldsFlag(Automatons.unionAndMinimize(automatonList), hasLegacyExceptionFields);
    }

    private static AutomatonWithLegacyExceptFieldsFlag buildPermittedFieldsAutomaton(
        final String[] grantedFields,
        final String[] deniedFields
    ) {
        Automaton grantedFieldsAutomaton;
        if (grantedFields == null || Arrays.stream(grantedFields).anyMatch(Regex::isMatchAllPattern)) {
            grantedFieldsAutomaton = Automatons.MATCH_ALL;
        } else {
            grantedFieldsAutomaton = Automatons.patterns(grantedFields);
        }

        Automaton deniedFieldsAutomaton;
        if (deniedFields == null || deniedFields.length == 0) {
            deniedFieldsAutomaton = Automatons.EMPTY;
        } else {
            deniedFieldsAutomaton = Automatons.patterns(deniedFields);
        }

        // short-circuit if all fields are allowed
        if (grantedFieldsAutomaton == Automatons.MATCH_ALL && deniedFieldsAutomaton == Automatons.EMPTY) {
            return new AutomatonWithLegacyExceptFieldsFlag(Automatons.MATCH_ALL, false);
        }

        grantedFieldsAutomaton = MinimizationOperations.minimize(grantedFieldsAutomaton, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
        deniedFieldsAutomaton = MinimizationOperations.minimize(deniedFieldsAutomaton, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);

        boolean hasLegacyExceptionFields = false;
        if (Automatons.subsetOf(deniedFieldsAutomaton, grantedFieldsAutomaton) == false) {
            if (false == deniedFieldsSubsetOfGrantedWithLegacyMetadataFields(grantedFieldsAutomaton, deniedFieldsAutomaton)) {
                throw new ElasticsearchSecurityException(
                    "Exceptions for field permissions must be a subset of the "
                        + "granted fields but ["
                        + Strings.arrayToCommaDelimitedString(deniedFields)
                        + "] is not a subset of ["
                        + Strings.arrayToCommaDelimitedString(grantedFields)
                        + "]"
                );
            }
            hasLegacyExceptionFields = true;
        }

        return new AutomatonWithLegacyExceptFieldsFlag(
            Automatons.minusAndMinimize(grantedFieldsAutomaton, deniedFieldsAutomaton),
            hasLegacyExceptionFields
        );
    }

    private static boolean deniedFieldsSubsetOfGrantedWithLegacyMetadataFields(
        Automaton grantedFieldsAutomaton,
        Automaton deniedFieldsAutomaton
    ) {
        final Automaton legacyMetadataFieldsAutomaton = Operations.concatenate(Automata.makeChar('_'), Automata.makeAnyString());
        final Automaton grantedFieldsWithLegacyMetadataFieldsAutomaton = Automatons.unionAndMinimize(
            grantedFieldsAutomaton,
            legacyMetadataFieldsAutomaton
        );
        return Automatons.subsetOf(deniedFieldsAutomaton, grantedFieldsWithLegacyMetadataFieldsAutomaton);
    }

    record AutomatonWithLegacyExceptFieldsFlag(Automaton automaton, boolean hasLegacyExceptFields) {}

    /**
     * Returns a field permissions instance where it is limited by the given field permissions.<br>
     * If the current and the other field permissions have field level security then it takes
     * an intersection of permitted fields.<br>
     * If none of the permissions have field level security enabled, then returns permissions
     * instance where all fields are allowed.
     *
     * @param limitedBy {@link FieldPermissions} used to limit current field permissions
     * @return {@link FieldPermissions}
     */
    public FieldPermissions limitFieldPermissions(FieldPermissions limitedBy) {
        if (hasFieldLevelSecurity() && limitedBy != null && limitedBy.hasFieldLevelSecurity()) {
            // TODO: cache the automaton computation with FieldPermissionsCache
            Automaton _permittedFieldsAutomaton = Automatons.intersectAndMinimize(getIncludeAutomaton(), limitedBy.getIncludeAutomaton());
            return new FieldPermissions(
                CollectionUtils.concatLists(fieldPermissionsDefinitions, limitedBy.fieldPermissionsDefinitions),
                new AutomatonWithLegacyExceptFieldsFlag(_permittedFieldsAutomaton, limitedBy.hasLegacyExceptFields || hasLegacyExceptFields)
            );
        } else if (limitedBy != null && limitedBy.hasFieldLevelSecurity()) {
            return new FieldPermissions(
                limitedBy.fieldPermissionsDefinitions,
                new AutomatonWithLegacyExceptFieldsFlag(limitedBy.getIncludeAutomaton(), limitedBy.hasLegacyExceptFields)
            );
        } else if (hasFieldLevelSecurity()) {
            return new FieldPermissions(
                fieldPermissionsDefinitions,
                new AutomatonWithLegacyExceptFieldsFlag(getIncludeAutomaton(), hasLegacyExceptFields)
            );
        }
        return FieldPermissions.DEFAULT;
    }

    /**
     * Returns true if this field permission policy allows access to the field and false if not.
     * fieldName can be a wildcard.
     */
    public boolean grantsAccessTo(String fieldName) {
        return permittedFieldsAutomatonIsTotal || fieldPredicate.test(fieldName);
    }

    public FieldPredicate fieldPredicate() {
        return fieldPredicate;
    }

    public List<FieldPermissionsDefinition> getFieldPermissionsDefinitions() {
        return fieldPermissionsDefinitions;
    }

    // public for testing
    public CharacterRunAutomaton getPermittedFieldsAutomaton() {
        return permittedFieldsAutomaton;
    }

    @Override
    public void buildCacheKey(StreamOutput out, DlsQueryEvaluationContext context) throws IOException {
        out.writeCollection(fieldPermissionsDefinitions, (o, fpd) -> fpd.buildCacheKey(o, context));
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

    Automaton getIncludeAutomaton() {
        return originalAutomaton;
    }

    public static boolean isAllowlistedMetadataField(String fieldName) {
        return PREDICATE.test(fieldName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldPermissions that = (FieldPermissions) o;
        return permittedFieldsAutomatonIsTotal == that.permittedFieldsAutomatonIsTotal
            && hasLegacyExceptFields == that.hasLegacyExceptFields
            && fieldPermissionsDefinitions.equals(that.fieldPermissionsDefinitions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldPermissionsDefinitions, permittedFieldsAutomatonIsTotal, hasLegacyExceptFields);
    }

    @Override
    public long ramBytesUsed() {
        return ramBytesUsed;
    }

    public boolean hasLegacyExceptFields() {
        return hasLegacyExceptFields;
    }
}
