/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.permission;

import org.elasticsearch.cluster.metadata.AliasOrIndex;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xpack.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.security.support.Automatons;

import java.util.ArrayList;
import java.util.Arrays;
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
            return Automatons.predicate(indices);
        };
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

    /**
     * Authorizes the provided action against the provided indices, given the current cluster metadata
     */
    public Map<String, IndicesAccessControl.IndexAccessControl> authorize(String action, Set<String> requestedIndicesOrAliases,
                                                                          MetaData metaData, FieldPermissionsCache fieldPermissionsCache) {
        // now... every index that is associated with the request, must be granted
        // by at least one indices permission group

        SortedMap<String, AliasOrIndex> allAliasesAndIndices = metaData.getAliasAndIndexLookup();
        Map<String, Set<FieldPermissions>> fieldPermissionsByIndex = new HashMap<>();
        Map<String, Set<BytesReference>> roleQueriesByIndex = new HashMap<>();
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
                        fieldPermissions.add(group.getFieldPermissions());
                        if (group.hasQuery()) {
                            Set<BytesReference> roleQueries = roleQueriesByIndex.computeIfAbsent(index, (k) -> new HashSet<>());
                            roleQueries.addAll(group.getQuery());
                        }
                    }
                }
            }

            if (concreteIndices.isEmpty()) {
                grantedBuilder.put(indexOrAlias, granted);
            } else {
                for (String concreteIndex : concreteIndices) {
                    grantedBuilder.put(concreteIndex, granted);
                }
            }
        }

        Map<String, IndicesAccessControl.IndexAccessControl> indexPermissions = new HashMap<>();
        for (Map.Entry<String, Boolean> entry : grantedBuilder.entrySet()) {
            String index = entry.getKey();
            Set<BytesReference> roleQueries = roleQueriesByIndex.get(index);
            if (roleQueries != null) {
                roleQueries = unmodifiableSet(roleQueries);
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
            this.indexNameMatcher = Automatons.predicate(indices);
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
    }
}
