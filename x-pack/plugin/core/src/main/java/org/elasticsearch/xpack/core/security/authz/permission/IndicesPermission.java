/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authz.permission;

import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.action.admin.indices.mapping.put.AutoPutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.xpack.core.security.authz.RestrictedIndices;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.core.security.support.Automatons;
import org.elasticsearch.xpack.core.security.support.StringMatcher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;

/**
 * A permission that is based on privileges for index related actions executed
 * on specific indices
 */
public final class IndicesPermission {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(IndicesPermission.class);

    public static final IndicesPermission NONE = new IndicesPermission(new RestrictedIndices(Automatons.EMPTY), Group.EMPTY_ARRAY);

    private static final Set<String> PRIVILEGE_NAME_SET_BWC_ALLOW_MAPPING_UPDATE = Set.of("create", "create_doc", "index", "write");

    private final Map<String, Predicate<IndexAbstraction>> allowedIndicesMatchersForAction = new ConcurrentHashMap<>();

    private final RestrictedIndices restrictedIndices;
    private final Group[] groups;
    private final boolean hasFieldOrDocumentLevelSecurity;

    public static class Builder {

        RestrictedIndices restrictedIndices;
        List<Group> groups = new ArrayList<>();

        public Builder(RestrictedIndices restrictedIndices) {
            this.restrictedIndices = restrictedIndices;
        }

        public Builder addGroup(
            IndexPrivilege privilege,
            FieldPermissions fieldPermissions,
            @Nullable Set<BytesReference> query,
            boolean allowRestrictedIndices,
            String... indices
        ) {
            groups.add(new Group(privilege, fieldPermissions, query, allowRestrictedIndices, restrictedIndices, indices));
            return this;
        }

        public IndicesPermission build() {
            return new IndicesPermission(restrictedIndices, groups.toArray(Group.EMPTY_ARRAY));
        }
    }

    private IndicesPermission(RestrictedIndices restrictedIndices, Group[] groups) {
        this.restrictedIndices = restrictedIndices;
        this.groups = groups;
        this.hasFieldOrDocumentLevelSecurity = Arrays.stream(groups).noneMatch(Group::isTotal)
            && Arrays.stream(groups).anyMatch(g -> g.hasQuery() || g.fieldPermissions.hasFieldLevelSecurity());
    }

    /**
     * This function constructs an index matcher that can be used to find indices allowed by
     * permissions groups.
     *
     * @param ordinaryIndices A list of ordinary indices. If this collection contains restricted indices,
     *                        according to the restrictedNamesAutomaton, they will not be matched.
     * @param restrictedIndices A list of restricted index names. All of these will be matched.
     * @return A matcher that will match all non-restricted index names in the ordinaryIndices
     * collection and all index names in the restrictedIndices collection.
     */
    private StringMatcher indexMatcher(Collection<String> ordinaryIndices, Collection<String> restrictedIndices) {
        StringMatcher matcher;
        if (ordinaryIndices.isEmpty()) {
            matcher = StringMatcher.of(restrictedIndices);
        } else {
            matcher = StringMatcher.of(ordinaryIndices);
            if (this.restrictedIndices != null) {
                matcher = matcher.and("<not-restricted>", name -> this.restrictedIndices.isRestricted(name) == false);
            }
            if (restrictedIndices.isEmpty() == false) {
                matcher = StringMatcher.of(restrictedIndices).or(matcher);
            }
        }
        return matcher;
    }

    public Group[] groups() {
        return groups;
    }

    /**
     * @return A predicate that will match all the indices that this permission
     * has the privilege for executing the given action on.
     */
    public Predicate<IndexAbstraction> allowedIndicesMatcher(String action) {
        return allowedIndicesMatchersForAction.computeIfAbsent(action, this::buildIndexMatcherPredicateForAction);
    }

    public boolean hasFieldOrDocumentLevelSecurity() {
        return hasFieldOrDocumentLevelSecurity;
    }

    private Predicate<IndexAbstraction> buildIndexMatcherPredicateForAction(String action) {
        final Set<String> ordinaryIndices = new HashSet<>();
        final Set<String> restrictedIndices = new HashSet<>();
        final Set<String> grantMappingUpdatesOnIndices = new HashSet<>();
        final Set<String> grantMappingUpdatesOnRestrictedIndices = new HashSet<>();
        final boolean isMappingUpdateAction = isMappingUpdateAction(action);
        for (final Group group : groups) {
            if (group.actionMatcher.test(action)) {
                if (group.allowRestrictedIndices) {
                    restrictedIndices.addAll(Arrays.asList(group.indices()));
                } else {
                    ordinaryIndices.addAll(Arrays.asList(group.indices()));
                }
            } else if (isMappingUpdateAction && containsPrivilegeThatGrantsMappingUpdatesForBwc(group)) {
                // special BWC case for certain privileges: allow put mapping on indices and aliases (but not on data streams), even if
                // the privilege definition does not currently allow it
                if (group.allowRestrictedIndices) {
                    grantMappingUpdatesOnRestrictedIndices.addAll(Arrays.asList(group.indices()));
                } else {
                    grantMappingUpdatesOnIndices.addAll(Arrays.asList(group.indices()));
                }
            }
        }
        final StringMatcher nameMatcher = indexMatcher(ordinaryIndices, restrictedIndices);
        final StringMatcher bwcSpecialCaseMatcher = indexMatcher(grantMappingUpdatesOnIndices, grantMappingUpdatesOnRestrictedIndices);
        return indexAbstraction -> nameMatcher.test(indexAbstraction.getName())
            || (indexAbstraction.getType() != IndexAbstraction.Type.DATA_STREAM
                && (indexAbstraction.getParentDataStream() == null)
                && bwcSpecialCaseMatcher.test(indexAbstraction.getName()));
    }

    /**
     * Checks if the permission matches the provided action, without looking at indices.
     * To be used in very specific cases where indices actions need to be authorized regardless of their indices.
     * The usecase for this is composite actions that are initially only authorized based on the action name (indices are not
     * checked on the coordinating node), and properly authorized later at the shard level checking their indices as well.
     */
    public boolean check(String action) {
        final boolean isMappingUpdateAction = isMappingUpdateAction(action);
        for (Group group : groups) {
            if (group.checkAction(action) || (isMappingUpdateAction && containsPrivilegeThatGrantsMappingUpdatesForBwc(group))) {
                return true;
            }
        }
        return false;
    }

    /**
     * For given index patterns and index privileges determines allowed privileges and creates an instance of {@link ResourcePrivilegesMap}
     * holding a map of resource to {@link ResourcePrivileges} where resource is index pattern and the map of index privilege to whether it
     * is allowed or not.
     *
     * @param checkForIndexPatterns check permission grants for the set of index patterns
     * @param allowRestrictedIndices if {@code true} then checks permission grants even for restricted indices by index matching
     * @param checkForPrivileges check permission grants for the set of index privileges
     * @return an instance of {@link ResourcePrivilegesMap}
     */
    public ResourcePrivilegesMap checkResourcePrivileges(
        Set<String> checkForIndexPatterns,
        boolean allowRestrictedIndices,
        Set<String> checkForPrivileges
    ) {
        final ResourcePrivilegesMap.Builder resourcePrivilegesMapBuilder = ResourcePrivilegesMap.builder();
        final Map<IndicesPermission.Group, Automaton> predicateCache = new HashMap<>();
        for (String forIndexPattern : checkForIndexPatterns) {
            Automaton checkIndexAutomaton = Automatons.patterns(forIndexPattern);
            if (false == allowRestrictedIndices && false == isConcreteRestrictedIndex(forIndexPattern)) {
                checkIndexAutomaton = Automatons.minusAndMinimize(checkIndexAutomaton, restrictedIndices.getAutomaton());
            }
            if (false == Operations.isEmpty(checkIndexAutomaton)) {
                Automaton allowedIndexPrivilegesAutomaton = null;
                for (Group group : groups) {
                    final Automaton groupIndexAutomaton = predicateCache.computeIfAbsent(group, Group::getIndexMatcherAutomaton);
                    if (Operations.subsetOf(checkIndexAutomaton, groupIndexAutomaton)) {
                        if (allowedIndexPrivilegesAutomaton != null) {
                            allowedIndexPrivilegesAutomaton = Automatons.unionAndMinimize(
                                Arrays.asList(allowedIndexPrivilegesAutomaton, group.privilege().getAutomaton())
                            );
                        } else {
                            allowedIndexPrivilegesAutomaton = group.privilege().getAutomaton();
                        }
                    }
                }
                for (String privilege : checkForPrivileges) {
                    IndexPrivilege indexPrivilege = IndexPrivilege.get(Collections.singleton(privilege));
                    if (allowedIndexPrivilegesAutomaton != null
                        && Operations.subsetOf(indexPrivilege.getAutomaton(), allowedIndexPrivilegesAutomaton)) {
                        resourcePrivilegesMapBuilder.addResourcePrivilege(forIndexPattern, privilege, Boolean.TRUE);
                    } else {
                        resourcePrivilegesMapBuilder.addResourcePrivilege(forIndexPattern, privilege, Boolean.FALSE);
                    }
                }
            } else {
                // the index pattern produced the empty automaton, presumably because the requested pattern expands exclusively inside the
                // restricted indices namespace - a namespace of indices that are normally hidden when granting/checking privileges - and
                // the pattern was not marked as `allowRestrictedIndices`. We try to anticipate this by considering _explicit_ restricted
                // indices even if `allowRestrictedIndices` is false.
                // TODO The `false` result is a _safe_ default but this is actually an error. Make it an error.
                for (String privilege : checkForPrivileges) {
                    resourcePrivilegesMapBuilder.addResourcePrivilege(forIndexPattern, privilege, Boolean.FALSE);
                }
            }
        }
        return resourcePrivilegesMapBuilder.build();
    }

    public Automaton allowedActionsMatcher(String index) {
        List<Automaton> automatonList = new ArrayList<>();
        for (Group group : groups) {
            if (group.indexNameMatcher.test(index)) {
                automatonList.add(group.privilege.getAutomaton());
            }
        }
        return automatonList.isEmpty() ? Automatons.EMPTY : Automatons.unionAndMinimize(automatonList);
    }

    /**
     * Represents the set of data required about an IndexAbstraction (index/alias/datastream) in order to perform authorization on that
     * object (including setting up the necessary data structures for Field and Document Level Security).
     */
    private static class IndexResource {
        /**
         * The name of the IndexAbstraction on which authorization is being performed
         */
        private final String name;

        /**
         * The IndexAbstraction on which authorization is being performed, or {@code null} if nothing in the cluster matches the name
         */
        @Nullable
        private final IndexAbstraction indexAbstraction;

        public Collection<String> concreteIndices;

        private IndexResource(String name, @Nullable IndexAbstraction abstraction) {
            assert name != null : "Resource name cannot be null";
            assert abstraction == null || abstraction.getName().equals(name)
                : "Index abstraction has unexpected name [" + abstraction.getName() + "] vs [" + name + "]";
            this.name = name;
            this.indexAbstraction = abstraction;
        }

        /**
         * @return {@code true} if-and-only-if this object is related to a data-stream, either by having a
         * {@link IndexAbstraction#getType()} of {@link IndexAbstraction.Type#DATA_STREAM} or by being the backing index for a
         * {@link IndexAbstraction#getParentDataStream()}  data-stream}.
         */
        public boolean isPartOfDataStream() {
            if (indexAbstraction == null) {
                return false;
            }
            return switch (indexAbstraction.getType()) {
                case DATA_STREAM -> true;
                case CONCRETE_INDEX -> indexAbstraction.getParentDataStream() != null;
                default -> false;
            };
        }

        /**
         * Check whether this object is covered by the provided permission {@link Group}.
         * For indices that are part of a data-stream, this checks both the index name and the parent data-stream name.
         * In all other cases, it checks the name of this object only.
         */
        public boolean checkIndex(Group group) {
            final IndexAbstraction.DataStream ds = indexAbstraction == null ? null : indexAbstraction.getParentDataStream();
            if (ds != null) {
                if (group.checkIndex(ds.getName())) {
                    return true;
                }
            }
            return group.checkIndex(name);
        }

        /**
         * @return the number of distinct objects to which this expansion refers.
         */
        public int size() {
            if (indexAbstraction == null) {
                return 1;
            } else if (indexAbstraction.getType() == IndexAbstraction.Type.CONCRETE_INDEX) {
                return 1;
            } else {
                return 1 + indexAbstraction.getIndices().size();
            }
        }

        public Collection<String> resolveConcreteIndices() {
            if (indexAbstraction == null) {
                return List.of();
            } else if (indexAbstraction.getType() == IndexAbstraction.Type.CONCRETE_INDEX) {
                return List.of(indexAbstraction.getName());
            } else {
                final List<Index> indices = indexAbstraction.getIndices();
                final List<String> concreteIndexNames = new ArrayList<>(indices.size());
                for (var idx : indices) {
                    concreteIndexNames.add(idx.getName());
                }
                return concreteIndexNames;
            }
        }

        public boolean canHaveBackingIndices() {
            return indexAbstraction != null && indexAbstraction.getType() != IndexAbstraction.Type.CONCRETE_INDEX;
        }
    }

    /**
     * Authorizes the provided action against the provided indices, given the current cluster metadata
     */
    public IndicesAccessControl authorize(
        String action,
        Set<String> requestedIndicesOrAliases,
        Map<String, IndexAbstraction> lookup,
        FieldPermissionsCache fieldPermissionsCache
    ) {
        // Short circuit if the indicesPermission allows all access to every index
        if (Arrays.stream(groups).anyMatch(Group::isTotal)) {
            return IndicesAccessControl.allowAll();
        }

        final List<IndexResource> resources = new ArrayList<>(requestedIndicesOrAliases.size());
        int totalResourceCount = 0;

        for (String indexOrAlias : requestedIndicesOrAliases) {
            final IndexResource resource = new IndexResource(indexOrAlias, lookup.get(indexOrAlias));
            resources.add(resource);
            totalResourceCount += resource.size();
        }

        // now... every index that is associated with the request, must be granted
        // by at least one indices permission group
        final Map<String, Set<FieldPermissions>> fieldPermissionsByIndex = Maps.newMapWithExpectedSize(totalResourceCount);
        final Map<String, DocumentLevelPermissions> roleQueriesByIndex = Maps.newMapWithExpectedSize(totalResourceCount);
        final Map<String, Boolean> grantedBuilder = Maps.newMapWithExpectedSize(totalResourceCount);

        final boolean isMappingUpdateAction = isMappingUpdateAction(action);

        for (IndexResource resource : resources) {
            // true if ANY group covers the given index AND the given action
            boolean granted = false;
            // true if ANY group, which contains certain ingest privileges, covers the given index AND the action is a mapping update for
            // an index or an alias (but not for a data stream)
            boolean bwcGrantMappingUpdate = false;
            final List<Runnable> bwcDeprecationLogActions = new ArrayList<>();

            final Collection<String> concreteIndices = resource.resolveConcreteIndices();
            for (Group group : groups) {
                // the group covers the given index OR the given index is a backing index and the group covers the parent data stream
                if (resource.checkIndex(group)) {
                    boolean actionCheck = group.checkAction(action);
                    granted = granted || actionCheck;

                    // mapping updates are allowed for certain privileges on indices and aliases (but not on data streams),
                    // outside of the privilege definition
                    boolean bwcMappingActionCheck = isMappingUpdateAction
                        && false == resource.isPartOfDataStream()
                        && containsPrivilegeThatGrantsMappingUpdatesForBwc(group);
                    bwcGrantMappingUpdate = bwcGrantMappingUpdate || bwcMappingActionCheck;

                    if (actionCheck || bwcMappingActionCheck) {
                        // propagate DLS and FLS permissions over the concrete indices
                        for (String index : concreteIndices) {
                            final Set<FieldPermissions> fieldPermissions = fieldPermissionsByIndex.compute(index, (k, existingSet) -> {
                                if (existingSet == null) {
                                    // Most indices rely on the default (empty) field permissions object, so we optimize for that case
                                    // Using an immutable single item set is significantly faster because it avoids any of the hashing
                                    // and backing set creation.
                                    return Set.of(group.getFieldPermissions());
                                } else if (existingSet.size() == 1) {
                                    FieldPermissions fp = group.getFieldPermissions();
                                    if (existingSet.contains(fp)) {
                                        return existingSet;
                                    }
                                    // This index doesn't have a single field permissions object, replace the singleton with a real Set
                                    final Set<FieldPermissions> hashSet = new HashSet<>(existingSet);
                                    hashSet.add(fp);
                                    return hashSet;
                                } else {
                                    existingSet.add(group.getFieldPermissions());
                                    return existingSet;
                                }
                            });

                            DocumentLevelPermissions docPermissions;
                            if (group.hasQuery()) {
                                docPermissions = roleQueriesByIndex.computeIfAbsent(index, (k) -> new DocumentLevelPermissions());
                                docPermissions.addAll(group.getQuery());
                            } else {
                                // if more than one permission matches for a concrete index here and if
                                // a single permission doesn't have a role query then DLS will not be
                                // applied even when other permissions do have a role query
                                docPermissions = DocumentLevelPermissions.ALLOW_ALL;
                                // don't worry about what's already there - just overwrite it, it avoids doing a 2nd hash lookup.
                                roleQueriesByIndex.put(index, docPermissions);
                            }

                            if (index.equals(resource.name) == false) {
                                fieldPermissionsByIndex.put(resource.name, fieldPermissions);
                                roleQueriesByIndex.put(resource.name, docPermissions);
                            }

                        }
                        if (false == actionCheck) {
                            for (String privilegeName : group.privilege.name()) {
                                if (PRIVILEGE_NAME_SET_BWC_ALLOW_MAPPING_UPDATE.contains(privilegeName)) {
                                    bwcDeprecationLogActions.add(
                                        () -> deprecationLogger.warn(
                                            DeprecationCategory.SECURITY,
                                            "[" + resource.name + "] mapping update for ingest privilege [" + privilegeName + "]",
                                            "the index privilege ["
                                                + privilegeName
                                                + "] allowed the update "
                                                + "mapping action ["
                                                + action
                                                + "] on index ["
                                                + resource.name
                                                + "], this privilege "
                                                + "will not permit mapping updates in the next major release - users who require access "
                                                + "to update mappings must be granted explicit privileges"
                                        )
                                    );
                                }
                            }
                        }
                    }
                }
            }

            if (false == granted && bwcGrantMappingUpdate) {
                // the action is granted only due to the deprecated behaviour of certain privileges
                granted = true;
                bwcDeprecationLogActions.forEach(Runnable::run);
            }

            grantedBuilder.put(resource.name, granted);
            if (resource.canHaveBackingIndices()) {
                for (String concreteIndex : concreteIndices) {
                    // If the name appear directly as part of the requested indices, it takes precedence over implicit access
                    if (false == requestedIndicesOrAliases.contains(concreteIndex)) {
                        grantedBuilder.merge(concreteIndex, granted, Boolean::logicalOr);
                    }
                }
            }
        }

        boolean overallGranted = true;
        Map<String, IndicesAccessControl.IndexAccessControl> indexPermissions = Maps.newMapWithExpectedSize(grantedBuilder.size());
        for (Map.Entry<String, Boolean> entry : grantedBuilder.entrySet()) {
            String index = entry.getKey();
            DocumentLevelPermissions permissions = roleQueriesByIndex.get(index);
            final Set<BytesReference> roleQueries;
            if (permissions != null && permissions.isAllowAll() == false) {
                roleQueries = unmodifiableSet(permissions.queries);
            } else {
                roleQueries = null;
            }

            final FieldPermissions fieldPermissions;
            final Set<FieldPermissions> indexFieldPermissions = fieldPermissionsByIndex.get(index);
            if (indexFieldPermissions != null && indexFieldPermissions.isEmpty() == false) {
                fieldPermissions = indexFieldPermissions.size() == 1
                    ? indexFieldPermissions.iterator().next()
                    : fieldPermissionsCache.getFieldPermissions(indexFieldPermissions);
            } else {
                fieldPermissions = FieldPermissions.DEFAULT;
            }
            if (entry.getValue() == false) {
                overallGranted = false;
            }
            indexPermissions.put(
                index,
                new IndicesAccessControl.IndexAccessControl(
                    entry.getValue(),
                    fieldPermissions,
                    (roleQueries != null) ? DocumentPermissions.filteredBy(roleQueries) : DocumentPermissions.allowAll()
                )
            );
        }
        return new IndicesAccessControl(overallGranted, unmodifiableMap(indexPermissions));
    }

    private boolean isConcreteRestrictedIndex(String indexPattern) {
        if (Regex.isSimpleMatchPattern(indexPattern) || Automatons.isLuceneRegex(indexPattern)) {
            return false;
        }
        return restrictedIndices.isRestricted(indexPattern);
    }

    private static boolean isMappingUpdateAction(String action) {
        return action.equals(PutMappingAction.NAME) || action.equals(AutoPutMappingAction.NAME);
    }

    private static boolean containsPrivilegeThatGrantsMappingUpdatesForBwc(Group group) {
        return group.privilege().name().stream().anyMatch(PRIVILEGE_NAME_SET_BWC_ALLOW_MAPPING_UPDATE::contains);
    }

    public static class Group {
        public static final Group[] EMPTY_ARRAY = new Group[0];

        private final IndexPrivilege privilege;
        private final Predicate<String> actionMatcher;
        private final String[] indices;
        private final StringMatcher indexNameMatcher;
        private final Supplier<Automaton> indexNameAutomaton;
        private final FieldPermissions fieldPermissions;
        private final Set<BytesReference> query;
        // by default certain restricted indices are exempted when granting privileges, as they should generally be hidden for ordinary
        // users. Setting this flag true eliminates the special status for the purpose of this permission - restricted indices still have
        // to be covered by the "indices"
        private final boolean allowRestrictedIndices;

        public Group(
            IndexPrivilege privilege,
            FieldPermissions fieldPermissions,
            @Nullable Set<BytesReference> query,
            boolean allowRestrictedIndices,
            RestrictedIndices restrictedIndices,
            String... indices
        ) {
            assert indices.length != 0;
            this.privilege = privilege;
            this.actionMatcher = privilege.predicate();
            this.indices = indices;
            this.allowRestrictedIndices = allowRestrictedIndices;
            ConcurrentHashMap<String[], Automaton> indexNameAutomatonMemo = new ConcurrentHashMap<>(1);
            if (allowRestrictedIndices) {
                this.indexNameMatcher = StringMatcher.of(indices);
                this.indexNameAutomaton = () -> indexNameAutomatonMemo.computeIfAbsent(indices, k -> Automatons.patterns(indices));
            } else {
                this.indexNameMatcher = StringMatcher.of(indices).and(name -> restrictedIndices.isRestricted(name) == false);
                this.indexNameAutomaton = () -> indexNameAutomatonMemo.computeIfAbsent(
                    indices,
                    k -> Automatons.minusAndMinimize(Automatons.patterns(indices), restrictedIndices.getAutomaton())
                );
            }
            this.fieldPermissions = Objects.requireNonNull(fieldPermissions);
            this.query = query;
        }

        public IndexPrivilege privilege() {
            return privilege;
        }

        public String[] indices() {
            return indices;
        }

        @Nullable
        public Set<BytesReference> getQuery() {
            return query;
        }

        public FieldPermissions getFieldPermissions() {
            return fieldPermissions;
        }

        private boolean checkAction(String action) {
            return actionMatcher.test(action);
        }

        private boolean checkIndex(String index) {
            assert index != null;
            return indexNameMatcher.test(index);
        }

        boolean hasQuery() {
            return query != null;
        }

        public boolean allowRestrictedIndices() {
            return allowRestrictedIndices;
        }

        public Automaton getIndexMatcherAutomaton() {
            return indexNameAutomaton.get();
        }

        boolean isTotal() {
            return allowRestrictedIndices
                && indexNameMatcher.isTotal()
                && privilege == IndexPrivilege.ALL
                && query == null
                && false == fieldPermissions.hasFieldLevelSecurity();
        }
    }

    private static class DocumentLevelPermissions {

        public static final DocumentLevelPermissions ALLOW_ALL = new DocumentLevelPermissions();
        static {
            ALLOW_ALL.allowAll = true;
        }

        private Set<BytesReference> queries = null;
        private boolean allowAll = false;

        private void addAll(Set<BytesReference> query) {
            if (allowAll == false) {
                if (queries == null) {
                    queries = new HashSet<>(query.size());
                }
                queries.addAll(query);
            }
        }

        private boolean isAllowAll() {
            return allowAll;
        }
    }
}
