/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authz.permission;

import org.apache.logging.log4j.LogManager;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.cluster.metadata.AliasOrIndex;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.SubsetResult.Result;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.core.security.support.Automatons;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;

import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;

/**
 * A permission that is based on privileges for index related actions executed
 * on specific indices
 */
public final class IndicesPermission implements Iterable<IndicesPermission.Group> {

    public static final IndicesPermission NONE = new IndicesPermission();

    private final Function<String, Predicate<String>> loadingFunction;

    private final ConcurrentHashMap<String, Predicate<String>> allowedIndicesMatchersForAction = new ConcurrentHashMap<>();

    private final Group[] groups;

    public IndicesPermission(Group... groups) {
        this.groups = groups;
        loadingFunction = (action) -> {
            List<String> indices = new ArrayList<>();
            for (Group group : groups) {
                if (group.actionMatcher.test(action)) {
                    indices.addAll(Arrays.asList(group.indices));
                }
            }
            return indexMatcher(indices);
        };
    }

    static Predicate<String> indexMatcher(List<String> indices) {
        try {
            return Automatons.predicate(indices);
        } catch (TooComplexToDeterminizeException e) {
            LogManager.getLogger(IndicesPermission.class).debug("Index pattern automaton [{}] is too complex", indices);
            String description = Strings.collectionToCommaDelimitedString(indices);
            if (description.length() > 80) {
                description = Strings.cleanTruncate(description, 80) + "...";
            }
            throw new ElasticsearchSecurityException("The set of permitted index patterns [{}] is too complex to evaluate", e, description);
        }
    }

    @Override
    public Iterator<Group> iterator() {
        return Arrays.asList(groups).iterator();
    }

    public Group[] groups() {
        return groups;
    }

    /**
     * @return A predicate that will match all the indices that this permission
     * has the privilege for executing the given action on.
     */
    public Predicate<String> allowedIndicesMatcher(String action) {
        return allowedIndicesMatchersForAction.computeIfAbsent(action, loadingFunction);
    }

    /**
     * Checks if the permission matches the provided action, without looking at indices.
     * To be used in very specific cases where indices actions need to be authorized regardless of their indices.
     * The usecase for this is composite actions that are initially only authorized based on the action name (indices are not
     * checked on the coordinating node), and properly authorized later at the shard level checking their indices as well.
     */
    public boolean check(String action) {
        for (Group group : groups) {
            if (group.check(action)) {
                return true;
            }
        }
        return false;
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
     * Determines if this {@link IndicesPermission} is a subset of other indices
     * permission. This iteratively determines if all of the
     * {@link IndicesPermission.Group} for this IndicesPermission is a subset of
     * some group in the given indices permission.
     *
     * @param other indices permission
     * @return in case it is not a subset then returns
     * {@link SubsetResult.Result#NO} and if it is clearly a subset will return
     * {@link SubsetResult.Result#YES}. It will return
     * {@link SubsetResult.Result#MAYBE} when the role is a subset in every
     * other aspect except DLS queries since we cannot determine if a query
     * returns a subset of documents
     */
    public SubsetResult isSubsetOf(final IndicesPermission other) {
        if (this.groups() == null || this.groups().length == 0) {
            return SubsetResult.isASubset();
        }
        Set<Set<String>> combineDLSQueriesFromIndexPrivilegeMatchingTheseNames = new HashSet<>();
        for (Group thisGroup : this.groups()) {
            boolean granted = false;
            Set<Set<String>> maybeGroupIndices = Collections.emptySet();
            if (other.groups() != null) {
                for (Group otherGroup : other.groups()) {
                    final SubsetResult result = thisGroup.isSubsetOf(otherGroup);
                    if (result.result() == Result.YES) {
                        granted = true;
                        maybeGroupIndices.clear();
                        break;
                    } else if (result.result() == Result.MAYBE) {
                        granted = true;
                        maybeGroupIndices = Sets.union(maybeGroupIndices, result.setOfIndexNamesForCombiningDLSQueries());
                    }
                }
            }
            if (granted == false) {
                return SubsetResult.isNotASubset();
            } else {
                combineDLSQueriesFromIndexPrivilegeMatchingTheseNames.addAll(maybeGroupIndices);
            }
        }
        if (combineDLSQueriesFromIndexPrivilegeMatchingTheseNames.isEmpty()) {
            return SubsetResult.isASubset();
        } else {
            return SubsetResult.mayBeASubset(combineDLSQueriesFromIndexPrivilegeMatchingTheseNames);
        }
    }

    /**
     * Authorizes the provided action against the provided indices, given the current cluster metadata
     */
    public Map<String, IndicesAccessControl.IndexAccessControl> authorize(String action, Set<String> requestedIndicesOrAliases,
                                                                          MetaData metaData, FieldPermissionsCache fieldPermissionsCache) {
        // now... every index that is associated with the request, must be granted
        // by at least one indices permission group

        SortedMap<String, AliasOrIndex> allAliasesAndIndices = metaData.getAliasAndIndexLookup();
        Map<String, Set<FieldPermissions>> fieldPermissionsByIndex = new HashMap<>();
        Map<String, DocumentLevelPermissions> roleQueriesByIndex = new HashMap<>();
        Map<String, Boolean> grantedBuilder = new HashMap<>();

        for (String indexOrAlias : requestedIndicesOrAliases) {
            boolean granted = false;
            Set<String> concreteIndices = new HashSet<>();
            AliasOrIndex aliasOrIndex = allAliasesAndIndices.get(indexOrAlias);
            if (aliasOrIndex != null) {
                for (IndexMetaData indexMetaData : aliasOrIndex.getIndices()) {
                    concreteIndices.add(indexMetaData.getIndex().getName());
                }
            }

            for (Group group : groups) {
                if (group.check(action, indexOrAlias)) {
                    granted = true;
                    for (String index : concreteIndices) {
                        Set<FieldPermissions> fieldPermissions = fieldPermissionsByIndex.computeIfAbsent(index, (k) -> new HashSet<>());
                        fieldPermissionsByIndex.put(indexOrAlias, fieldPermissions);
                        fieldPermissions.add(group.getFieldPermissions());
                        DocumentLevelPermissions permissions =
                                roleQueriesByIndex.computeIfAbsent(index, (k) -> new DocumentLevelPermissions());
                        roleQueriesByIndex.putIfAbsent(indexOrAlias, permissions);
                        if (group.hasQuery()) {
                            permissions.addAll(group.getQuery());
                        } else {
                            // if more than one permission matches for a concrete index here and if
                            // a single permission doesn't have a role query then DLS will not be
                            // applied even when other permissions do have a role query
                            permissions.setAllowAll(true);
                        }
                    }
                }
            }

            if (concreteIndices.isEmpty()) {
                grantedBuilder.put(indexOrAlias, granted);
            } else {
                grantedBuilder.put(indexOrAlias, granted);
                for (String concreteIndex : concreteIndices) {
                    grantedBuilder.put(concreteIndex, granted);
                }
            }
        }

        Map<String, IndicesAccessControl.IndexAccessControl> indexPermissions = new HashMap<>();
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
                fieldPermissions = indexFieldPermissions.size() == 1 ? indexFieldPermissions.iterator().next() :
                        fieldPermissionsCache.getFieldPermissions(indexFieldPermissions);
            } else {
                fieldPermissions = FieldPermissions.DEFAULT;
            }
            indexPermissions.put(index, new IndicesAccessControl.IndexAccessControl(entry.getValue(), fieldPermissions, roleQueries));
        }
        return unmodifiableMap(indexPermissions);
    }

    public static class Group {
        private final IndexPrivilege privilege;
        private final Predicate<String> actionMatcher;
        private final String[] indices;
        private final Predicate<String> indexNameMatcher;

        public FieldPermissions getFieldPermissions() {
            return fieldPermissions;
        }

        private final FieldPermissions fieldPermissions;
        private final Set<BytesReference> query;

        public Group(IndexPrivilege privilege, FieldPermissions fieldPermissions, @Nullable Set<BytesReference> query, String... indices) {
            assert indices.length != 0;
            this.privilege = privilege;
            this.actionMatcher = privilege.predicate();
            this.indices = indices;
            this.indexNameMatcher = indexMatcher(Arrays.asList(indices));
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

        private boolean check(String action) {
            return actionMatcher.test(action);
        }

        private boolean check(String action, String index) {
            assert index != null;
            return check(action) && indexNameMatcher.test(index);
        }

        boolean hasQuery() {
            return query != null;
        }

        public SubsetResult isSubsetOf(Group other) {
            final boolean isSubsetExcludingDls = areIndicesASubset(other) && arePrivilegesASubset(other)
                    && areFieldPermissionsASubset(other);
            if (isSubsetExcludingDls) {
                return isDlsASubset(other);
            } else {
                return SubsetResult.isNotASubset();
            }
        }

        private SubsetResult isDlsASubset(Group other) {
            SubsetResult result;
            if (null == this.getQuery() && null == other.getQuery()) {
                result = SubsetResult.isASubset();
            } else if (this.getQuery() != null && other.getQuery() == null) {
                result = SubsetResult.isASubset();
            } else if (this.getQuery() == null && other.getQuery() != null) {
                result = SubsetResult.mayBeASubset(Collections.singleton(Sets.newHashSet(this.indices())));
            } else if (Sets.difference(this.getQuery(), other.getQuery()).isEmpty()) {
                result = SubsetResult.isASubset();
            } else {
                result = SubsetResult.mayBeASubset(Collections.singleton(Sets.newHashSet(this.indices())));
            }
            return result;
        }

        private boolean areFieldPermissionsASubset(Group other) {
            final Automaton thisFieldsPermissionAutomaton = FieldPermissions
                    .initializePermittedFieldsAutomaton(this.getFieldPermissions().getFieldPermissionsDefinition());
            final Automaton otherFieldsPermissionAutomaton = FieldPermissions
                    .initializePermittedFieldsAutomaton(other.getFieldPermissions().getFieldPermissionsDefinition());
            return Operations.subsetOf(thisFieldsPermissionAutomaton, otherFieldsPermissionAutomaton);
        }

        private boolean arePrivilegesASubset(Group other) {
            return Operations.subsetOf(this.privilege().getAutomaton(), other.privilege().getAutomaton());
        }

        private boolean areIndicesASubset(Group other) {
            return Operations.subsetOf(Automatons.patterns(this.indices()), Automatons.patterns(other.indices()));
        }
    }

    private static class DocumentLevelPermissions {

        private Set<BytesReference> queries = null;
        private boolean allowAll = false;

        private void addAll(Set<BytesReference> query) {
            if (allowAll == false) {
                if (queries == null) {
                    queries = new HashSet<>();
                }
                queries.addAll(query);
            }
        }

        private boolean isAllowAll() {
            return allowAll;
        }

        private void setAllowAll(boolean allowAll) {
            this.allowAll = allowAll;
        }
    }
}
