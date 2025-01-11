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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege.READ;
import static org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege.READ_FAILURES;
import static org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege.READ_FAILURES_PRIVILEGE_NAME;

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
        IsResourceAuthorizedPredicate predicate = new IsResourceAuthorizedPredicate((name, abstraction) -> AuthorizedComponents.NONE);
        for (final Group group : groups) {
            predicate = predicate.orAllowIf(group.allowedIndicesPredicate(action));
        }
        return predicate;
    }

    /**
     * This encapsulates the authorization test for resources.
     * There is an additional test for resources that are missing or that are not a datastream or a backing index.
     */
    public static class IsResourceAuthorizedPredicate {

        private final BiFunction<String, IndexAbstraction, AuthorizedComponents> biPredicate;

        public IsResourceAuthorizedPredicate(BiFunction<String, IndexAbstraction, AuthorizedComponents> biPredicate) {
            this.biPredicate = biPredicate;
        }

        /**
        * Given another {@link IsResourceAuthorizedPredicate} instance in {@param other},
        * return a new {@link IsResourceAuthorizedPredicate} instance that is equivalent to the conjunction of
        * authorization tests of that other instance and this one.
        */
        // @Override
        public final IsResourceAuthorizedPredicate orAllowIf(IsResourceAuthorizedPredicate other) {
            Objects.requireNonNull(other);
            return new IsResourceAuthorizedPredicate((name, abstraction) -> {
                AuthorizedComponents authResult = this.biPredicate.apply(name, abstraction);
                // If we're only authorized for some components, other predicates might authorize us for the rest
                return switch (authResult) {
                    case null -> other.biPredicate.apply(name, abstraction);
                    case NONE -> other.biPredicate.apply(name, abstraction); // Can't do worse than totally unauthorized, thank u NEXT
                    case ALL -> AuthorizedComponents.ALL; // Can't do better than ALL, so short circuit
                    case DATA, FAILURES -> authResult.union(other.biPredicate.apply(name, abstraction));
                };
            });
        }

        public final IsResourceAuthorizedPredicate alsoRequire(IsResourceAuthorizedPredicate other) {
            Objects.requireNonNull(other);
            return new IsResourceAuthorizedPredicate((name, abstraction) -> {
                AuthorizedComponents authResult = this.biPredicate.apply(name, abstraction);
                // If we're only authorized for some components, other predicates might authorize us for the rest
                return switch (authResult) {
                    case null -> AuthorizedComponents.NONE;
                    case NONE -> AuthorizedComponents.NONE;
                    case ALL -> other.biPredicate.apply(name, abstraction);
                    case DATA, FAILURES -> authResult.intersection(other.biPredicate.apply(name, abstraction));
                };
            });
        }

        /**
         * Check which components of the given {@param indexAbstraction} resource is authorized.
         * The resource must exist. Otherwise, use the {@link #check(String, IndexAbstraction)} method.
         * @return An object representing which components of this index abstraction the user is authorized to access
         */
        public final AuthorizedComponents check(IndexAbstraction indexAbstraction) {
            return check(indexAbstraction.getName(), indexAbstraction);
        }

        public final boolean test(IndexAbstraction indexAbstraction) {
            AuthorizedComponents authResult = check(indexAbstraction);
            return authResult != null && authResult.isDataAuthorized();
        }

        /**
         * Verifies if access is authorized to the resource with the given {@param name}.
         * The {@param indexAbstraction}, which is the resource to be accessed, must be supplied if the resource exists or be {@code null}
         * if it doesn't.
         * Returns {@code true} if access to the given resource is authorized or {@code false} otherwise.
         */
        public AuthorizedComponents check(String name, @Nullable IndexAbstraction indexAbstraction) {
            return biPredicate.apply(name, indexAbstraction);
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
                                failureIndices += parentDataStream.getFailureIndices().getIndices().size();
                            }
                        }
                        size += failureIndices;
                    } else {
                        DataStream parentDataStream = (DataStream) indexAbstraction;
                        size += parentDataStream.getFailureIndices().getIndices().size();
                    }
                }
                return size;
            } else {
                return 1 + indexAbstraction.getIndices().size();
            }
        }

        public Collection<String> resolveConcreteIndices(Map<String, IndexAbstraction> lookup) {
            if (indexAbstraction == null) {
                return List.of();
            } else if (indexAbstraction.getType() == IndexAbstraction.Type.CONCRETE_INDEX) {
                return List.of(indexAbstraction.getName());
            } else if (IndexComponentSelector.FAILURES.equals(selector)) {
                if (IndexAbstraction.Type.ALIAS.equals(indexAbstraction.getType())) {
                    Set<DataStream> aliasDataStreams = new HashSet<>();
                    for (Index index : indexAbstraction.getIndices()) {
                        DataStream parentDataStream = lookup.get(index.getName()).getParentDataStream();
                        if (parentDataStream != null) {
                            aliasDataStreams.add(parentDataStream);
                        }
                    }
                    List<String> concreteIndexNames = new ArrayList<>(aliasDataStreams.size());
                    for (DataStream aliasDataStream : aliasDataStreams) {
                        DataStream.DataStreamIndices failureIndices = aliasDataStream.getFailureIndices();
                        for (Index index : failureIndices.getIndices()) {
                            concreteIndexNames.add(index.getName());
                        }
                    }
                    return concreteIndexNames;
                } else {
                    DataStream parentDataStream = (DataStream) indexAbstraction;
                    DataStream.DataStreamIndices failureIndices = parentDataStream.getFailureIndices();
                    List<String> concreteIndexNames = new ArrayList<>(failureIndices.getIndices().size());
                    for (Index index : failureIndices.getIndices()) {
                        concreteIndexNames.add(index.getName());
                    }
                    return concreteIndexNames;
                }
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
        for (Group group : groups) {
            if (group.isTotal()) {
                return IndicesAccessControl.allowAll();
            }
        }

        final Map<String, IndexResource> resources = Maps.newMapWithExpectedSize(requestedIndicesOrAliases.size());
        int totalResourceCount = 0;

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
            lookup
        );

        return new IndicesAccessControl(overallGranted, indexPermissions);
    }

    private Map<String, IndicesAccessControl.IndexAccessControl> buildIndicesAccessControl(
        final String action,
        final Map<String, IndexResource> requestedResources,
        final int totalResourceCount,
        final FieldPermissionsCache fieldPermissionsCache,
        final Map<String, IndexAbstraction> lookup
    ) {

        // now... every index that is associated with the request, must be granted
        // by at least one indices permission group
        final Map<String, Set<FieldPermissions>> fieldPermissionsByIndex = Maps.newMapWithExpectedSize(totalResourceCount);
        final Map<String, DocumentLevelPermissions> roleQueriesByIndex = Maps.newMapWithExpectedSize(totalResourceCount);
        final Set<String> grantedResources = Sets.newHashSetWithExpectedSize(totalResourceCount);

        for (IndexResource resource : requestedResources.values()) {
            // true if ANY group covers the given index AND the given action
            boolean granted = false;

            final Collection<String> concreteIndices = resource.resolveConcreteIndices(lookup);
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

    private static boolean isMappingUpdateAction(String action) {
        return action.equals(TransportPutMappingAction.TYPE.name()) || action.equals(TransportAutoPutMappingAction.TYPE.name());
    }

    private static boolean containsPrivilegeThatGrantsMappingUpdatesForBwc(Set<String> names) {
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
        // These two flags are just a cache so we don't have to constantly re-check strings
        private final boolean hasMappingUpdateBwcPermissions;
        private final boolean hasReadFailuresPrivilege;

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
            this.hasMappingUpdateBwcPermissions = containsPrivilegeThatGrantsMappingUpdatesForBwc(privilege.name());
            this.hasReadFailuresPrivilege = this.privilege.name().stream().anyMatch(READ_FAILURES_PRIVILEGE_NAME::equals);
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

        IsResourceAuthorizedPredicate allowedIndicesPredicate(String action) {
            return new IsResourceAuthorizedPredicate(allowedIndicesChecker(action));
        }

        /**
         * Returns a checker which checks if users are permitted to access index abstractions for the given action.
         * @param action
         * @return A function which checks an index name and returns the components of this abstraction the user is authorized to access
         *          with the given action.
         */
        private BiFunction<String, IndexAbstraction, AuthorizedComponents> allowedIndicesChecker(String action) {
            boolean isReadAction = READ.predicate().test(action);
            boolean isMappingUpdateAction = isMappingUpdateAction(action);
            boolean actionMatches = actionMatcher.test(action);
            boolean actionAuthorized = actionMatches || (isReadAction && hasReadFailuresPrivilege);
            if (actionAuthorized == false) {
                AtomicBoolean logged = new AtomicBoolean(false);
                return (name, resource) -> {
                    if (isMappingUpdateAction
                        && hasMappingUpdateBwcPermissions
                        && resource != null
                        && resource.getParentDataStream() == null
                        && resource.getType() != IndexAbstraction.Type.DATA_STREAM) {
                        boolean alreadyLogged = logged.getAndSet(true);
                        if (alreadyLogged == false) {
                            for (String privilegeName : this.privilege.name()) {
                                if (PRIVILEGE_NAME_SET_BWC_ALLOW_MAPPING_UPDATE.contains(privilegeName)) {
                                    // ATHE: Does this log more often?
                                    deprecationLogger.warn(
                                        DeprecationCategory.SECURITY,
                                        "[" + resource.getName() + "] mapping update for ingest privilege [" + privilegeName + "]",
                                        "the index privilege ["
                                            + privilegeName
                                            + "] allowed the update "
                                            + "mapping action ["
                                            + action
                                            + "] on index ["
                                            + resource.getName()
                                            + "], this privilege "
                                            + "will not permit mapping updates in the next major release - users who require access "
                                            + "to update mappings must be granted explicit privileges"
                                    );
                                }
                            }
                        }
                        return AuthorizedComponents.ALL;
                    }
                    return AuthorizedComponents.NONE;
                };
            }

            return (String abstractionName, IndexAbstraction resource) -> {
                assert resource == null || abstractionName.equals(resource.getName());
                return switch (resource.getType()) {
                    case ALIAS -> checkMultiIndexAbstraction(isReadAction, actionMatches, resource);
                    case DATA_STREAM -> checkMultiIndexAbstraction(isReadAction, actionMatches, resource);
                    case CONCRETE_INDEX -> {
                        IndexAbstraction.ConcreteIndex index = (IndexAbstraction.ConcreteIndex) resource;
                        final DataStream ds = index.getParentDataStream();

                        if (ds != null) {
                            boolean isFailureStoreIndex = ds.getFailureIndices().containsIndex(resource.getName());

                            if (isReadAction) {
                                // If we're trying to read a failure store index, we need to have read_failures for the data stream
                                if (isFailureStoreIndex) {
                                    if ((hasReadFailuresPrivilege && indexNameMatcher.test(ds.getName()))) {
                                        yield AuthorizedComponents.FAILURES; // And authorize it as a failures index (i.e. no DLS/FLS)
                                    }
                                } else { // not a failure store index
                                    if (indexNameMatcher.test(ds.getName()) || indexNameMatcher.test(resource.getName())) {
                                        yield AuthorizedComponents.DATA;
                                    }
                                }
                            } else { // Not a read action, authenticate as normal
                                if (checkParentNameAndResourceName(ds.getName(), resource.getName())) {
                                    yield AuthorizedComponents.DATA;
                                }
                            }
                        } else if (indexNameMatcher.test(resource.getName())) {
                            yield AuthorizedComponents.DATA;
                        }
                        yield AuthorizedComponents.NONE;
                    }
                    case null -> indexNameMatcher.test(abstractionName) ? AuthorizedComponents.DATA : AuthorizedComponents.NONE;
                };
            };
        }

        private boolean checkParentNameAndResourceName(String dsName, String indexName) {
            return indexNameMatcher.test(dsName) || indexNameMatcher.test(indexName);
        }

        private AuthorizedComponents checkMultiIndexAbstraction(boolean isReadAction, boolean actionMatches, IndexAbstraction resource) {
            if (indexNameMatcher.test(resource.getName())) {
                if (actionMatches && (isReadAction == false || hasReadFailuresPrivilege)) {
                    // User has both normal read privileges and read_failures OR normal privileges and action is not read
                    return AuthorizedComponents.ALL;
                } else if (actionMatches && hasReadFailuresPrivilege == false) {
                    return AuthorizedComponents.DATA;
                } else if (hasReadFailuresPrivilege) { // action not authorized by typical match
                    return AuthorizedComponents.FAILURES;
                }
            }
            return AuthorizedComponents.NONE;
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
