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
import org.elasticsearch.action.support.IndexComponentSelector;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.bytes.BytesReference;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege.READ_FAILURES;
import static org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege.READ_FAILURES_PRIVILEGE_NAME;

/**
 * A permission that is based on privileges for index related actions executed
 * on specific indices
 */
public final class IndicesPermission {
    public static final IndicesPermission NONE = new IndicesPermission(new RestrictedIndices(Automatons.EMPTY), Group.EMPTY_ARRAY);

    static final Set<String> PRIVILEGE_NAME_SET_BWC_ALLOW_MAPPING_UPDATE = Set.of("create", "create_doc", "index", "write");

    private final Map<String, IsResourceAuthorizedPredicate> allowedIndicesMatchersForAction = new ConcurrentHashMap<>();

    private final RestrictedIndices restrictedIndices;
    private final Group[] groups;
    private final boolean hasFieldOrDocumentLevelSecurity;

    public enum AuthorizedComponents {
        NONE,
        DATA,
        FAILURES,
        ALL;

        public AuthorizedComponents union(AuthorizedComponents other) {
            if (other == null) {
                return this;
            }
            return switch (this) {
                case ALL -> ALL;
                case NONE -> other;
                case DATA -> other.isFailuresAuthorized() ? ALL : DATA;
                case FAILURES -> other.isDataAuthorized() ? ALL : FAILURES;
            };
        }

        public AuthorizedComponents intersection(AuthorizedComponents other) {
            if (other == null) {
                return NONE;
            }
            return switch (this) {
                case ALL -> other;
                case NONE -> NONE;
                case DATA -> other.isDataAuthorized() ? DATA : NONE;
                case FAILURES -> other.isFailuresAuthorized() ? FAILURES : NONE;
            };
        }

        /**
         * @return True if data components are authorized for the resource in question
         */
        public boolean isDataAuthorized() {
            return switch (this) {
                case ALL, DATA -> true;
                case NONE, FAILURES -> false;
            };
        }

        /**
         * @return True if failure components are authorized for the resource in question
         */
        public boolean isFailuresAuthorized() {
            return switch (this) {
                case ALL, FAILURES -> true;
                case NONE, DATA -> false;
            };
        }

        /**
         * @return True if any components, data or failure, are authorized for the resource in question. Mostly for visibility checks.
         */
        public boolean isAnyAuthorized() {
            return switch (this) {
                case ALL, DATA, FAILURES -> true;
                case NONE -> false;
            };
        }
    }

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
        this.hasFieldOrDocumentLevelSecurity = Arrays.stream(groups).noneMatch(Group::allowsTotalDataIndexAccess)
            && Arrays.stream(groups).anyMatch(g -> g.hasQuery() || g.getFieldPermissions().hasFieldLevelSecurity());
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
        if (groups.length == 0) {
            return new IsResourceAuthorizedPredicate.NoResourcesAuthorizedChecker();
        }

        IsResourceAuthorizedPredicate predicate = groups[0].allowedIndicesPredicate(action);
        for (int i = 1; i < groups.length; i++) {
            predicate = predicate.orAllowIf(groups[i].allowedIndicesPredicate(action));
        }

        return predicate;
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
            if (group.checkAction(action)
                || (isMappingUpdateAction && containsPrivilegeThatGrantsMappingUpdatesForBwc(group.privilege().name()))) {
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
                    if (Automatons.subsetOf(checkIndexAutomaton, indexAndPrivilegeAutomaton.getValue())) {
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
                        && Automatons.subsetOf(indexPrivilege.getAutomaton(), allowedIndexPrivilegesAutomaton)) {
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
         * The selector to be applied to the IndexAbstraction which selects which indices to return when resolving
         */
        @Nullable
        private final IndexComponentSelector selector;

        /**
         * The IndexAbstraction on which authorization is being performed, or {@code null} if nothing in the cluster matches the name
         */
        @Nullable
        private final IndexAbstraction indexAbstraction;

        private IndexResource(String name, @Nullable IndexAbstraction abstraction, @Nullable IndexComponentSelector selector) {
            assert name != null : "Resource name cannot be null";
            assert abstraction == null || abstraction.getName().equals(name)
                : "Index abstraction has unexpected name [" + abstraction.getName() + "] vs [" + name + "]";
            assert abstraction == null
                || selector == null
                || IndexComponentSelector.FAILURES.equals(selector) == false
                || abstraction.isDataStreamRelated()
                : "Invalid index component selector ["
                    + selector.getKey()
                    + "] applied to abstraction of type ["
                    + abstraction.getType()
                    + "]";
            this.name = name;
            this.indexAbstraction = abstraction;
            this.selector = selector;
        }

        /**
         * @return the number of distinct objects to which this expansion refers.
         */
        public int size(Map<String, IndexAbstraction> lookup) {
            if (indexAbstraction == null) {
                return 1;
            } else if (indexAbstraction.getType() == IndexAbstraction.Type.CONCRETE_INDEX) {
                return 1;
            } else if (selector != null) {
                int size = 1;
                if (selector.shouldIncludeData()) {
                    size += indexAbstraction.getIndices().size();
                }
                if (selector.shouldIncludeFailures()) {
                    if (IndexAbstraction.Type.ALIAS.equals(indexAbstraction.getType())) {
                        Set<DataStream> aliasDataStreams = new HashSet<>();
                        int failureIndices = 0;
                        for (Index index : indexAbstraction.getIndices()) {
                            DataStream parentDataStream = lookup.get(index.getName()).getParentDataStream();
                            if (parentDataStream != null && aliasDataStreams.add(parentDataStream)) {
                                failureIndices += parentDataStream.getFailureIndices().size();
                            }
                        }
                        size += failureIndices;
                    } else {
                        DataStream parentDataStream = (DataStream) indexAbstraction;
                        size += parentDataStream.getFailureIndices().size();
                    }
                }
                return size;
            } else {
                return 1 + indexAbstraction.getIndices().size();
            }
        }

        public Collection<String> resolveConcreteIndices(Metadata metadata) {
            if (indexAbstraction == null) {
                return List.of();
            } else if (indexAbstraction.getType() == IndexAbstraction.Type.CONCRETE_INDEX) {
                return List.of(indexAbstraction.getName());
            } else if (IndexComponentSelector.FAILURES.equals(selector)) {
                final List<Index> failureIndices = indexAbstraction.getFailureIndices(metadata);
                final List<String> concreteIndexNames = new ArrayList<>(failureIndices.size());
                for (var idx : failureIndices) {
                    concreteIndexNames.add(idx.getName());
                }
                return concreteIndexNames;
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
        Metadata metadata,
        FieldPermissionsCache fieldPermissionsCache
    ) {
        final Map<String, IndexResource> resources = Maps.newMapWithExpectedSize(requestedIndicesOrAliases.size());
        int totalResourceCount = 0;
        Map<String, IndexAbstraction> lookup = metadata.getIndicesLookup();
        for (String indexOrAlias : requestedIndicesOrAliases) {
            // Remove any selectors from abstraction name. Discard them for this check as we do not have access control for them (yet)
            Tuple<String, String> expressionAndSelector = IndexNameExpressionResolver.splitSelectorExpression(indexOrAlias);
            indexOrAlias = expressionAndSelector.v1();
            IndexComponentSelector selector = expressionAndSelector.v2() == null
                ? null
                : IndexComponentSelector.getByKey(expressionAndSelector.v2());

            final IndexResource resource = new IndexResource(indexOrAlias, lookup.get(indexOrAlias), selector);
            resources.put(resource.name, resource);
            totalResourceCount += resource.size(lookup);
        }

        final boolean overallGranted = isActionGranted(action, resources);
        final int finalTotalResourceCount = totalResourceCount;
        final Supplier<Map<String, IndicesAccessControl.IndexAccessControl>> indexPermissions = () -> buildIndicesAccessControl(
            action,
            resources,
            finalTotalResourceCount,
            fieldPermissionsCache,
            metadata
        );

        return new IndicesAccessControl(overallGranted, indexPermissions);
    }

    private Map<String, IndicesAccessControl.IndexAccessControl> buildIndicesAccessControl(
        final String action,
        final Map<String, IndexResource> requestedResources,
        final int totalResourceCount,
        final FieldPermissionsCache fieldPermissionsCache,
        final Metadata metadata
    ) {

        // now... every index that is associated with the request, must be granted
        // by at least one index permission group
        final Map<String, Set<FieldPermissions>> fieldPermissionsByIndex = Maps.newMapWithExpectedSize(totalResourceCount);
        final Map<String, DocumentLevelPermissions> roleQueriesByIndex = Maps.newMapWithExpectedSize(totalResourceCount);
        final Set<String> grantedResources = Sets.newHashSetWithExpectedSize(totalResourceCount);

        for (IndexResource resource : requestedResources.values()) {
            // true if ANY group covers the given index AND the given action
            boolean granted = false;

            final Collection<String> concreteIndices = resource.resolveConcreteIndices(metadata);
            for (Group group : groups) {
                AuthorizedComponents authorizedComponents = group.allowedIndicesPredicate(action)
                    .check(resource.name, resource.indexAbstraction);
                // the group covers the given index OR the given index is a backing index and the group covers the parent data stream
                if (authorizedComponents != null && authorizedComponents != AuthorizedComponents.NONE) {
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
        for (IndexResource resource : requestedResources.values()) {
            // true if ANY group covers the given index AND the given action
            boolean granted = false;

            for (Group group : groups) {
                AuthorizedComponents authResult = group.allowedIndicesPredicate(action).check(resource.name, resource.indexAbstraction);
                if (authResult != null && authResult.isAnyAuthorized()) {
                    granted = true;
                    break;
                }
            }

            if (granted == false) {
                // We stop and return at first not granted resource.
                return false;
            }
        }

        // None of the above resources were rejected.
        return true;
    }

    private boolean isConcreteRestrictedIndex(String indexPattern) {
        if (Regex.isSimpleMatchPattern(indexPattern) || Automatons.isLuceneRegex(indexPattern)) {
            return false;
        }
        return restrictedIndices.isRestricted(indexPattern);
    }

    static boolean isMappingUpdateAction(String action) {
        return action.equals(TransportPutMappingAction.TYPE.name()) || action.equals(TransportAutoPutMappingAction.TYPE.name());
    }

    static boolean containsPrivilegeThatGrantsMappingUpdatesForBwc(Set<String> names) {
        return names.stream().anyMatch(PRIVILEGE_NAME_SET_BWC_ALLOW_MAPPING_UPDATE::contains);
    }

    private static boolean isFailureReadAction(String action) {
        return READ_FAILURES.predicate().test(action);
    }

    private static boolean containsReadFailuresPrivilege(Group group) {
        return group.privilege().name().contains(READ_FAILURES_PRIVILEGE_NAME);
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
