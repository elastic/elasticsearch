/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz.permission;

import org.elasticsearch.cluster.metadata.AliasOrIndex;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.shield.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.shield.authz.privilege.IndexPrivilege;
import org.elasticsearch.shield.support.AutomatonPredicate;
import org.elasticsearch.shield.support.Automatons;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;

/**
 * A permission that is based on privileges for index related actions executed
 * on specific indices
 */
public interface IndicesPermission extends Permission, Iterable<IndicesPermission.Group> {

    Map<String, IndicesAccessControl.IndexAccessControl> authorize(String action, Set<String> requestedIndicesOrAliases, MetaData metaData);

    public static class Core implements IndicesPermission {

        public static final Core NONE = new Core() {
            @Override
            public Iterator<Group> iterator() {
                return Collections.emptyIterator();
            }

            @Override
            public boolean isEmpty() {
                return true;
            }
        };

        private final Function<String, Predicate<String>> loadingFunction;

        private final ConcurrentHashMap<String, Predicate<String>> allowedIndicesMatchersForAction = new ConcurrentHashMap<>();

        private final Group[] groups;

        public Core(List<Group> groups) {
            this(groups.toArray(new Group[groups.size()]));
        }

        public Core(Group... groups) {
            this.groups = groups;
            loadingFunction = (action) -> {
                List<String> indices = new ArrayList<>();
                for (Group group : groups) {
                    if (group.actionMatcher.test(action)) {
                        indices.addAll(Arrays.asList(group.indices));
                    }
                }
                return new AutomatonPredicate(Automatons.patterns(Collections.unmodifiableList(indices)));
            };
        }

        @Override
        public Iterator<Group> iterator() {
            return Arrays.asList(groups).iterator();
        }

        public Group[] groups() {
            return groups;
        }

        @Override
        public boolean isEmpty() {
            return groups == null || groups.length == 0;
        }

        /**
         * @return A predicate that will match all the indices that this permission
         * has the privilege for executing the given action on.
         */
        public Predicate<String> allowedIndicesMatcher(String action) {
            return allowedIndicesMatchersForAction.computeIfAbsent(action, loadingFunction);
        }

        @Override
        public Map<String, IndicesAccessControl.IndexAccessControl> authorize(String action, Set<String> requestedIndicesOrAliases,
                                                                              MetaData metaData) {
            // now... every index that is associated with the request, must be granted
            // by at least one indices permission group

            SortedMap<String, AliasOrIndex> allAliasesAndIndices = metaData.getAliasAndIndexLookup();
            Map<String, Set<String>> rolesFieldsByIndex = new HashMap<>();
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
                            if (group.getFields() != null) {
                                Set<String> roleFields = rolesFieldsByIndex.get(index);
                                if (roleFields == null) {
                                    roleFields = new HashSet<>();
                                    rolesFieldsByIndex.put(index, roleFields);
                                }
                                roleFields.addAll(group.getFields());
                            }
                            if (group.getQuery() != null) {
                                Set<BytesReference> roleQueries = roleQueriesByIndex.get(index);
                                if (roleQueries == null) {
                                    roleQueries = new HashSet<>();
                                    roleQueriesByIndex.put(index, roleQueries);
                                }
                                roleQueries.add(group.getQuery());
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
                Set<String> roleFields = rolesFieldsByIndex.get(index);
                if (roleFields != null) {
                    roleFields = unmodifiableSet(roleFields);
                }
                indexPermissions.put(index, new IndicesAccessControl.IndexAccessControl(entry.getValue(), roleFields, roleQueries));
            }
            return unmodifiableMap(indexPermissions);
        }

    }

    class Globals implements IndicesPermission {

        private final List<GlobalPermission> globals;

        public Globals(List<GlobalPermission> globals) {
            this.globals = globals;
        }

        @Override
        public Iterator<Group> iterator() {
            return globals == null || globals.isEmpty() ?
                    Collections.<Group>emptyIterator() :
                    new Globals.Iter(globals);
        }

        @Override
        public boolean isEmpty() {
            if (globals == null || globals.isEmpty()) {
                return true;
            }
            for (GlobalPermission global : globals) {
                if (!global.indices().isEmpty()) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public Map<String, IndicesAccessControl.IndexAccessControl> authorize(String action, Set<String> requestedIndicesOrAliases,
                                                                              MetaData metaData) {
            if (isEmpty()) {
                return emptyMap();
            }

            // What this code does is just merge `IndexAccessControl` instances from the permissions this class holds:
            Map<String, IndicesAccessControl.IndexAccessControl> indicesAccessControl = null;
            for (GlobalPermission permission : globals) {
                Map<String, IndicesAccessControl.IndexAccessControl> temp = permission.indices().authorize(action,
                        requestedIndicesOrAliases, metaData);
                if (indicesAccessControl == null) {
                    indicesAccessControl = new HashMap<>(temp);
                } else {
                    for (Map.Entry<String, IndicesAccessControl.IndexAccessControl> entry : temp.entrySet()) {
                        IndicesAccessControl.IndexAccessControl existing = indicesAccessControl.get(entry.getKey());
                        if (existing != null) {
                            indicesAccessControl.put(entry.getKey(), existing.merge(entry.getValue()));
                        } else {
                            indicesAccessControl.put(entry.getKey(), entry.getValue());
                        }
                    }
                }
            }
            if (indicesAccessControl == null) {
                return emptyMap();
            } else {
                return unmodifiableMap(indicesAccessControl);
            }
        }

        static class Iter implements Iterator<Group> {

            private final Iterator<GlobalPermission> globals;
            private Iterator<Group> current;

            Iter(List<GlobalPermission> globals) {
                this.globals = globals.iterator();
                advance();
            }

            @Override
            public boolean hasNext() {
                return current != null && current.hasNext();
            }

            @Override
            public Group next() {
                Group group = current.next();
                advance();
                return group;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

            private void advance() {
                if (current != null && current.hasNext()) {
                    return;
                }
                if (!globals.hasNext()) {
                    // we've reached the end of the globals array
                    current = null;
                    return;
                }

                while (globals.hasNext()) {
                    IndicesPermission indices = globals.next().indices();
                    if (!indices.isEmpty()) {
                        current = indices.iterator();
                        return;
                    }
                }

                current = null;
            }
        }
    }

    class Group {
        private final IndexPrivilege privilege;
        private final Predicate<String> actionMatcher;
        private final String[] indices;
        private final Predicate<String> indexNameMatcher;
        private final List<String> fields;
        private final BytesReference query;

        public Group(IndexPrivilege privilege, @Nullable List<String> fields, @Nullable BytesReference query, String... indices) {
            assert indices.length != 0;
            this.privilege = privilege;
            this.actionMatcher = privilege.predicate();
            this.indices = indices;
            this.indexNameMatcher = new AutomatonPredicate(Automatons.patterns(indices));
            this.fields = fields;
            this.query = query;
        }

        public IndexPrivilege privilege() {
            return privilege;
        }

        public String[] indices() {
            return indices;
        }

        @Nullable
        public List<String> getFields() {
            return fields;
        }

        @Nullable
        public BytesReference getQuery() {
            return query;
        }

        public boolean indexNameMatch(String index) {
            return indexNameMatcher.test(index);
        }

        public boolean check(String action, String index) {
            assert index != null;
            return actionMatcher.test(action) && indexNameMatcher.test(index);
        }
    }
}
