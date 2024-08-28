/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authz.permission;

import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.action.admin.indices.mapping.put.TransportAutoPutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.TransportPutMappingAction;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static java.util.Collections.unmodifiableMap;

/**
 * A permission that is based on privileges for index related actions executed
 * on specific indices
 */
public final class IndicesPermission {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(IndicesPermission.class);

    public static final IndicesPermission NONE = new IndicesPermission(new RestrictedIndices(Automatons.EMPTY), Group.EMPTY_ARRAY);

    private static final Set<String> PRIVILEGE_NAME_SET_BWC_ALLOW_MAPPING_UPDATE = Set.of("create", "create_doc", "index", "write");

    private final Map<String, IsResourceAuthorizedPredicate> allowedIndicesMatchersForAction = new ConcurrentHashMap<>();

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
    public IsResourceAuthorizedPredicate allowedIndicesMatcher(String action) {
        return allowedIndicesMatchersForAction.computeIfAbsent(action, this::buildIndexMatcherPredicateForAction);
    }

    public boolean hasFieldOrDocumentLevelSecurity() {
        return hasFieldOrDocumentLevelSecurity;
    }

    private IsResourceAuthorizedPredicate buildIndexMatcherPredicateForAction(String action) {
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
        return new IsResourceAuthorizedPredicate(nameMatcher, bwcSpecialCaseMatcher);
    }

    /**
     * This encapsulates the authorization test for resources.
     * There is an additional test for resources that are missing or that are not a datastream or a backing index.
     */
    public static class IsResourceAuthorizedPredicate implements BiPredicate<String, IndexAbstraction> {

        private final BiPredicate<String, IndexAbstraction> biPredicate;

        // public for tests
        public IsResourceAuthorizedPredicate(StringMatcher resourceNameMatcher, StringMatcher additionalNonDatastreamNameMatcher) {
            this((String name, @Nullable IndexAbstraction indexAbstraction) -> {
                assert indexAbstraction == null || name.equals(indexAbstraction.getName());
                return resourceNameMatcher.test(name)
                    || (isPartOfDatastream(indexAbstraction) == false && additionalNonDatastreamNameMatcher.test(name));
            });
        }

        private IsResourceAuthorizedPredicate(BiPredicate<String, IndexAbstraction> biPredicate) {
            this.biPredicate = biPredicate;
        }

        /**
        * Given another {@link IsResourceAuthorizedPredicate} instance in {@param other},
        * return a new {@link IsResourceAuthorizedPredicate} instance that is equivalent to the conjunction of
        * authorization tests of that other instance and this one.
        */
        @Override
        public final IsResourceAuthorizedPredicate and(BiPredicate<? super String, ? super IndexAbstraction> other) {
            return new IsResourceAuthorizedPredicate(this.biPredicate.and(other));
        }

        /**
         * Verifies if access is authorized to the given {@param indexAbstraction} resource.
         * The resource must exist. Otherwise, use the {@link #test(String, IndexAbstraction)} method.
         * Returns {@code true} if access to the given resource is authorized or {@code false} otherwise.
         */
        public final boolean test(IndexAbstraction indexAbstraction) {
            return test(indexAbstraction.getName(), indexAbstraction);
        }

        /**
         * Verifies if access is authorized to the resource with the given {@param name}.
         * The {@param indexAbstraction}, which is the resource to be accessed, must be supplied if the resource exists or be {@code null}
         * if it doesn't.
         * Returns {@code true} if access to the given resource is authorized or {@code false} otherwise.
         */
        @Override
        public boolean test(String name, @Nullable IndexAbstraction indexAbstraction) {
            return biPredicate.test(name, indexAbstraction);
        }

        private static boolean isPartOfDatastream(IndexAbstraction indexAbstraction) {
            return indexAbstraction != null
                && (indexAbstraction.getType() == IndexAbstraction.Type.DATA_STREAM || indexAbstraction.getParentDataStream() != null);
        }
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

    public boolean checkResourcePrivileges(
        Set<String> checkForIndexPatterns,
        boolean allowRestrictedIndices,
        Set<String> checkForPrivileges,
        @Nullable ResourcePrivilegesMap.Builder resourcePrivilegesMapBuilder
    ) {
        return checkResourcePrivileges(
            checkForIndexPatterns,
            allowRestrictedIndices,
            checkForPrivileges,
            false,
            resourcePrivilegesMapBuilder
        );
    }

    /**
     * For given index patterns and index privileges determines allowed privileges and creates an instance of {@link ResourcePrivilegesMap}
     * holding a map of resource to {@link ResourcePrivileges} where resource is index pattern and the map of index privilege to whether it
     * is allowed or not.
     *
     * @param checkForIndexPatterns check permission grants for the set of index patterns
     * @param allowRestrictedIndices if {@code true} then checks permission grants even for restricted indices by index matching
     * @param checkForPrivileges check permission grants for the set of index privileges
     * @param combineIndexGroups combine index groups to enable checking against regular expressions
     * @param resourcePrivilegesMapBuilder out-parameter for returning the details on which privilege over which resource is granted or not.
     *                                     Can be {@code null} when no such details are needed so the method can return early, after
     *                                     encountering the first privilege that is not granted over some resource.
     * @return {@code true} when all the privileges are granted over all the resources, or {@code false} otherwise
     */
    public boolean checkResourcePrivileges(
        Set<String> checkForIndexPatterns,
        boolean allowRestrictedIndices,
        Set<String> checkForPrivileges,
        boolean combineIndexGroups,
        @Nullable ResourcePrivilegesMap.Builder resourcePrivilegesMapBuilder
    ) {
        boolean allMatch = true;
        Map<Automaton, Automaton> indexGroupAutomatons = indexGroupAutomatons(
            combineIndexGroups && checkForIndexPatterns.stream().anyMatch(Automatons::isLuceneRegex)
        );
        for (String forIndexPattern : checkForIndexPatterns) {
            Automaton checkIndexAutomaton = Automatons.patterns(forIndexPattern);
            if (false == allowRestrictedIndices && false == isConcreteRestrictedIndex(forIndexPattern)) {
                checkIndexAutomaton = Automatons.minusAndMinimize(checkIndexAutomaton, restrictedIndices.getAutomaton());
            }
            if (false == Operations.isEmpty(checkIndexAutomaton)) {
                Automaton allowedIndexPrivilegesAutomaton = null;
                for (var indexAndPrivilegeAutomaton : indexGroupAutomatons.entrySet()) {
                    if (Operations.subsetOf(checkIndexAutomaton, indexAndPrivilegeAutomaton.getValue())) {
                        if (allowedIndexPrivilegesAutomaton != null) {
                            allowedIndexPrivilegesAutomaton = Automatons.unionAndMinimize(
                                Arrays.asList(allowedIndexPrivilegesAutomaton, indexAndPrivilegeAutomaton.getKey())
                            );
                        } else {
                            allowedIndexPrivilegesAutomaton = indexAndPrivilegeAutomaton.getKey();
                        }
                    }
                }
                for (String privilege : checkForPrivileges) {
                    IndexPrivilege indexPrivilege = IndexPrivilege.get(Collections.singleton(privilege));
                    if (allowedIndexPrivilegesAutomaton != null
                        && Operations.subsetOf(indexPrivilege.getAutomaton(), allowedIndexPrivilegesAutomaton)) {
                        if (resourcePrivilegesMapBuilder != null) {
                            resourcePrivilegesMapBuilder.addResourcePrivilege(forIndexPattern, privilege, Boolean.TRUE);
                        }
                    } else {
                        if (resourcePrivilegesMapBuilder != null) {
                            resourcePrivilegesMapBuilder.addResourcePrivilege(forIndexPattern, privilege, Boolean.FALSE);
                            allMatch = false;
                        } else {
                            // return early on first privilege not granted
                            return false;
                        }
                    }
                }
            } else {
                // the index pattern produced the empty automaton, presumably because the requested pattern expands exclusively inside the
                // restricted indices namespace - a namespace of indices that are normally hidden when granting/checking privileges - and
                // the pattern was not marked as `allowRestrictedIndices`. We try to anticipate this by considering _explicit_ restricted
                // indices even if `allowRestrictedIndices` is false.
                // TODO The `false` result is a _safe_ default but this is actually an error. Make it an error.
                if (resourcePrivilegesMapBuilder != null) {
                    for (String privilege : checkForPrivileges) {
                        resourcePrivilegesMapBuilder.addResourcePrivilege(forIndexPattern, privilege, Boolean.FALSE);
                    }
                    allMatch = false;
                } else {
                    // return early on first privilege not granted
                    return false;
                }
            }
        }
        return allMatch;
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
            final DataStream ds = indexAbstraction == null ? null : indexAbstraction.getParentDataStream();
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

        final Map<String, IndexResource> resources = Maps.newMapWithExpectedSize(requestedIndicesOrAliases.size());
        final AtomicInteger totalResourceCountHolder = new AtomicInteger(0);

        for (String indexOrAlias : requestedIndicesOrAliases) {
            final IndexResource resource = new IndexResource(indexOrAlias, lookup.get(indexOrAlias));
            resources.put(resource.name, resource);
            totalResourceCountHolder.getAndAdd(resource.size());
        }

        final boolean overallGranted = isActionGranted(action, resources);

        final Supplier<Map<String, IndicesAccessControl.IndexAccessControl>> indexPermissions = () -> buildIndicesAccessControl(
            action,
            resources,
            totalResourceCountHolder.get(),
            fieldPermissionsCache
        );

        return new IndicesAccessControl(overallGranted, indexPermissions);
    }

    private Map<String, IndicesAccessControl.IndexAccessControl> buildIndicesAccessControl(
        final String action,
        final Map<String, IndexResource> requestedResources,
        final int totalResourceCount,
        final FieldPermissionsCache fieldPermissionsCache
    ) {

        // now... every index that is associated with the request, must be granted
        // by at least one indices permission group
        final Map<String, Set<FieldPermissions>> fieldPermissionsByIndex = Maps.newMapWithExpectedSize(totalResourceCount);
        final Map<String, DocumentLevelPermissions> roleQueriesByIndex = Maps.newMapWithExpectedSize(totalResourceCount);
        final Set<String> grantedResources = Sets.newHashSetWithExpectedSize(totalResourceCount);

        final boolean isMappingUpdateAction = isMappingUpdateAction(action);

        for (IndexResource resource : requestedResources.values()) {
            // true if ANY group covers the given index AND the given action
            boolean granted = false;

            final Collection<String> concreteIndices = resource.resolveConcreteIndices();
            for (Group group : groups) {
                // the group covers the given index OR the given index is a backing index and the group covers the parent data stream
                if (resource.checkIndex(group)) {
                    if (group.checkAction(action)
                        || (isMappingUpdateAction // for BWC reasons, mapping updates are exceptionally allowed for certain privileges on
                            // indices and aliases (but not on data streams)
                            && false == resource.isPartOfDataStream()
                            && containsPrivilegeThatGrantsMappingUpdatesForBwc(group))) {
                        granted = true;
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
                    }
                }
            }

            if (granted) {
                grantedResources.add(resource.name);
                if (resource.canHaveBackingIndices()) {
                    for (String concreteIndex : concreteIndices) {
                        // If the name appear directly as part of the requested indices, it takes precedence over implicit access
                        if (false == requestedResources.containsKey(concreteIndex)) {
                            grantedResources.add(concreteIndex);
                        }
                    }
                }
            }
        }

        Map<String, IndicesAccessControl.IndexAccessControl> indexPermissions = Maps.newMapWithExpectedSize(grantedResources.size());
        for (String index : grantedResources) {
            final DocumentLevelPermissions permissions = roleQueriesByIndex.get(index);
            final DocumentPermissions documentPermissions;
            if (permissions != null && permissions.isAllowAll() == false) {
                documentPermissions = DocumentPermissions.filteredBy(permissions.queries);
            } else {
                documentPermissions = DocumentPermissions.allowAll();
            }
            final FieldPermissions fieldPermissions;
            final Set<FieldPermissions> indexFieldPermissions = fieldPermissionsByIndex.get(index);
            if (indexFieldPermissions != null && indexFieldPermissions.isEmpty() == false) {
                fieldPermissions = indexFieldPermissions.size() == 1
                    ? indexFieldPermissions.iterator().next()
                    : fieldPermissionsCache.union(indexFieldPermissions);
            } else {
                fieldPermissions = FieldPermissions.DEFAULT;
            }
            indexPermissions.put(index, new IndicesAccessControl.IndexAccessControl(fieldPermissions, documentPermissions));
        }
        return unmodifiableMap(indexPermissions);
    }

    /**
     * Returns {@code true} if action is granted for all {@code requestedResources}.
     * If action is not granted for at least one resource, this method will return {@code false}.
     */
    private boolean isActionGranted(final String action, final Map<String, IndexResource> requestedResources) {

        final boolean isMappingUpdateAction = isMappingUpdateAction(action);

        for (IndexResource resource : requestedResources.values()) {
            // true if ANY group covers the given index AND the given action
            boolean granted = false;
            // true if ANY group, which contains certain ingest privileges, covers the given index AND the action is a mapping update for
            // an index or an alias (but not for a data stream)
            boolean bwcGrantMappingUpdate = false;
            final List<Runnable> bwcDeprecationLogActions = new ArrayList<>();

            for (Group group : groups) {
                // the group covers the given index OR the given index is a backing index and the group covers the parent data stream
                if (resource.checkIndex(group)) {
                    boolean actionCheck = group.checkAction(action);
                    // If action is granted we don't have to check for BWC and can stop at first granting group.
                    if (actionCheck) {
                        granted = true;
                        break;
                    } else {
                        // mapping updates are allowed for certain privileges on indices and aliases (but not on data streams),
                        // outside of the privilege definition
                        boolean bwcMappingActionCheck = isMappingUpdateAction
                            && false == resource.isPartOfDataStream()
                            && containsPrivilegeThatGrantsMappingUpdatesForBwc(group);
                        bwcGrantMappingUpdate = bwcGrantMappingUpdate || bwcMappingActionCheck;

                        if (bwcMappingActionCheck) {
                            logDeprecatedBwcPrivilegeUsage(action, resource, group, bwcDeprecationLogActions);
                        }
                    }
                }
            }

            if (false == granted && bwcGrantMappingUpdate) {
                // the action is granted only due to the deprecated behaviour of certain privileges
                granted = true;
                bwcDeprecationLogActions.forEach(Runnable::run);
            }

            if (granted == false) {
                // We stop and return at first not granted resource.
                return false;
            }
        }

        // None of the above resources were rejected.
        return true;
    }

    private static void logDeprecatedBwcPrivilegeUsage(
        String action,
        IndexResource resource,
        Group group,
        List<Runnable> bwcDeprecationLogActions
    ) {
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

    private boolean isConcreteRestrictedIndex(String indexPattern) {
        if (Regex.isSimpleMatchPattern(indexPattern) || Automatons.isLuceneRegex(indexPattern)) {
            return false;
        }
        return restrictedIndices.isRestricted(indexPattern);
    }

    private static boolean isMappingUpdateAction(String action) {
        return action.equals(TransportPutMappingAction.TYPE.name()) || action.equals(TransportAutoPutMappingAction.TYPE.name());
    }

    private static boolean containsPrivilegeThatGrantsMappingUpdatesForBwc(Group group) {
        return group.privilege().name().stream().anyMatch(PRIVILEGE_NAME_SET_BWC_ALLOW_MAPPING_UPDATE::contains);
    }

    /**
     * Get all automatons for the index groups in this permission and optionally combine the index groups to enable checking if a set of
     * index patterns specified using a regular expression grants a set of index privileges.
     *
     * <p>An index group is defined as a set of index patterns and a set of privileges (excluding field permissions and DLS queries).
     * {@link IndicesPermission} consist of a set of index groups. For non-regular expression privilege checks, an index pattern is checked
     * against each index group, to see if it's a sub-pattern of the index pattern for the group and then if that group grants some or all
     * of the privileges requested. For regular expressions it's not sufficient to check per group since the index patterns covered by a
     * group can be distinct sets and a regular expression can cover several distinct sets.
     *
     * <p>For example the two index groups: {"names": ["a"], "privileges": ["read", "create"]} and {"names": ["b"],
     * "privileges": ["read","delete"]} will not match on ["\[ab]\"], while a single index group:
     * {"names": ["a", "b"], "privileges": ["read"]} will. This happens because the index groups are evaluated against a request index
     * pattern without first being combined. In the example above, the two index patterns should be combined to:
     * {"names": ["a", "b"], "privileges": ["read"]} before being checked.
     *
     *
     * @param combine combine index groups to allow for checking against regular expressions
     *
     * @return a map of all index and privilege pattern automatons
     */
    private Map<Automaton, Automaton> indexGroupAutomatons(boolean combine) {
        // Map of privilege automaton object references (cached by IndexPrivilege::CACHE)
        Map<Automaton, Automaton> allAutomatons = new HashMap<>();
        for (Group group : groups) {
            Automaton indexAutomaton = group.getIndexMatcherAutomaton();
            allAutomatons.compute(
                group.privilege().getAutomaton(),
                (key, value) -> value == null ? indexAutomaton : Automatons.unionAndMinimize(List.of(value, indexAutomaton))
            );
            if (combine) {
                List<Tuple<Automaton, Automaton>> combinedAutomatons = new ArrayList<>();
                for (var indexAndPrivilegeAutomatons : allAutomatons.entrySet()) {
                    Automaton intersectingPrivileges = Operations.intersection(
                        indexAndPrivilegeAutomatons.getKey(),
                        group.privilege().getAutomaton()
                    );
                    if (Operations.isEmpty(intersectingPrivileges) == false) {
                        Automaton indexPatternAutomaton = Automatons.unionAndMinimize(
                            List.of(indexAndPrivilegeAutomatons.getValue(), indexAutomaton)
                        );
                        combinedAutomatons.add(new Tuple<>(intersectingPrivileges, indexPatternAutomaton));
                    }
                }
                combinedAutomatons.forEach(
                    automatons -> allAutomatons.compute(
                        automatons.v1(),
                        (key, value) -> value == null ? automatons.v2() : Automatons.unionAndMinimize(List.of(value, automatons.v2()))
                    )
                );
            }
        }
        return allAutomatons;
    }

    public static class Group {
        public static final Group[] EMPTY_ARRAY = new Group[0];

        private final IndexPrivilege privilege;
        private final Predicate<String> actionMatcher;
        private final String[] indices;
        private final StringMatcher indexNameMatcher;
        private final Supplier<Automaton> indexNameAutomaton;
        // TODO: Use FieldPermissionsDefinition instead of FieldPermissions. The former is a better counterpart to query
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

        @Override
        public String toString() {
            return "Group{"
                + "privilege="
                + privilege
                + ", indices="
                + Strings.arrayToCommaDelimitedString(indices)
                + ", fieldPermissions="
                + fieldPermissions
                + ", query="
                + query
                + ", allowRestrictedIndices="
                + allowRestrictedIndices
                + '}';
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
                    queries = Sets.newHashSetWithExpectedSize(query.size());
                }
                queries.addAll(query);
            }
        }

        private boolean isAllowAll() {
            return allowAll;
        }
    }
}
