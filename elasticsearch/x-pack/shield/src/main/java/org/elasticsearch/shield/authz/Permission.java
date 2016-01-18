/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz;

import org.elasticsearch.cluster.metadata.AliasOrIndex;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.shield.authz.accesscontrol.IndicesAccessControl;
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
 * Represents a permission in the system. There are 3 types of permissions:
 *
 * <ul>
 *     <li>
 *         Cluster -    a permission that is based on privileges for cluster wide actions
 *     </li>
 *     <li>
 *         Indices -    a permission that is based on privileges for index related actions executed
 *                      on specific indices
 *     </li>
 *     <li>RunAs  -     a permissions that is based on a general privilege that contains patterns of users that this
 *                      user can execute a request as
 *     </li>
 *     <li>
 *         Global -     a composite permission that combines a both cluster &amp; indices permissions
 *     </li>
 * </ul>
 */
public interface Permission {

    boolean isEmpty();

    class Global implements Permission {

        public static final Global NONE = new Global(Cluster.Core.NONE, Indices.Core.NONE, RunAs.Core.NONE);

        private final Cluster cluster;
        private final Indices indices;
        private final RunAs runAs;

        Global(Cluster cluster, Indices indices, RunAs runAs) {
            this.cluster = cluster;
            this.indices = indices;
            this.runAs = runAs;
        }

        public Cluster cluster() {
            return cluster;
        }

        public Indices indices() {
            return indices;
        }

        public RunAs runAs() {
            return runAs;
        }

        @Override
        public boolean isEmpty() {
            return (cluster == null || cluster.isEmpty()) && (indices == null || indices.isEmpty()) && (runAs == null || runAs.isEmpty());
        }

        /**
         * Returns whether at least group encapsulated by this indices permissions is auhorized the execute the
         * specified action with the requested indices/aliases. At the same time if field and/or document level security
         * is configured for any group also the allowed fields and role queries are resolved.
         */
        public IndicesAccessControl authorize(String action, Set<String> requestedIndicesOrAliases, MetaData metaData) {
            Map<String, IndicesAccessControl.IndexAccessControl> indexPermissions = indices.authorize(
                    action, requestedIndicesOrAliases, metaData
            );

            // At least one role / indices permission set need to match with all the requested indices/aliases:
            boolean granted = true;
            for (Map.Entry<String, IndicesAccessControl.IndexAccessControl> entry : indexPermissions.entrySet()) {
                if (!entry.getValue().isGranted()) {
                    granted = false;
                    break;
                }
            }
            return new IndicesAccessControl(granted, indexPermissions);
        }

        public static class Role extends Global {

            private final String name;

            private Role(String name, Cluster.Core cluster, Indices.Core indices, RunAs.Core runAs) {
                super(cluster, indices, runAs);
                this.name = name;
            }

            public String name() {
                return name;
            }

            @Override
            public Cluster.Core cluster() {
                return (Cluster.Core) super.cluster();
            }

            @Override
            public Indices.Core indices() {
                return (Indices.Core) super.indices();
            }

            @Override
            public RunAs.Core runAs() {
                return (RunAs.Core) super.runAs();
            }

            public static Builder builder(String name) {
                return new Builder(name);
            }

            public static Builder builder(RoleDescriptor rd) {
                return new Builder(rd);
            }

            public static class Builder {

                private final String name;
                private Cluster.Core cluster = Cluster.Core.NONE;
                private RunAs.Core runAs = RunAs.Core.NONE;
                private List<Indices.Group> groups = new ArrayList<>();

                private Builder(String name) {
                    this.name = name;
                }

                private Builder(RoleDescriptor rd) {
                    this.name = rd.getName();
                    this.cluster(Privilege.Cluster.get((new Privilege.Name(rd.getClusterPattern()))));
                    for (RoleDescriptor.IndicesPrivileges iGroup : rd.getIndicesPrivileges()) {
                        this.add(iGroup.getFields() == null ? null : Arrays.asList(iGroup.getFields()),
                                iGroup.getQuery(),
                                Privilege.Index.get(new Privilege.Name(iGroup.getPrivileges())),
                                iGroup.getIndices());
                    }
                    String[] rdRunAs = rd.getRunAs();
                    if (rdRunAs != null && rdRunAs.length > 0) {
                        this.runAs(new Privilege.General(new Privilege.Name(rdRunAs), rdRunAs));
                    }
                }

                // FIXME we should throw an exception if we have already set cluster or runAs...
                public Builder cluster(Privilege.Cluster privilege) {
                    cluster = new Cluster.Core(privilege);
                    return this;
                }

                public Builder runAs(Privilege.General privilege) {
                    runAs = new RunAs.Core(privilege);
                    return this;
                }

                public Builder add(Privilege.Index privilege, String... indices) {
                    groups.add(new Indices.Group(privilege, null, null, indices));
                    return this;
                }

                public Builder add(List<String> fields, BytesReference query, Privilege.Index privilege, String... indices) {
                    groups.add(new Indices.Group(privilege, fields, query, indices));
                    return this;
                }

                public Role build() {
                    Indices.Core indices = groups.isEmpty() ? Indices.Core.NONE : new Indices.Core(groups.toArray(new Indices.Group[groups.size()]));
                    return new Role(name, cluster, indices, runAs);
                }
            }
        }

        static class Compound extends Global {

            public Compound(List<Global> globals) {
                super(new Cluster.Globals(globals), new Indices.Globals(globals), new RunAs.Globals(globals));
            }

            public static Builder builder() {
                return new Builder();
            }

            public static class Builder {

                private List<Global> globals = new ArrayList<>();

                private Builder() {
                }

                public Builder add(Global global) {
                    globals.add(global);
                    return this;
                }

                public Compound build() {
                    return new Compound(Collections.unmodifiableList(globals));
                }
            }
        }
    }

    static interface Cluster extends Permission {

        boolean check(String action);

        public static class Core implements Cluster {

            public static final Core NONE = new Core(Privilege.Cluster.NONE) {
                @Override
                public boolean check(String action) {
                    return false;
                }

                @Override
                public boolean isEmpty() {
                    return true;
                }
            };

            private final Privilege.Cluster privilege;
            private final Predicate<String> predicate;

            private Core(Privilege.Cluster privilege) {
                this.privilege = privilege;
                this.predicate = privilege.predicate();
            }

            public Privilege.Cluster privilege() {
                return privilege;
            }

            @Override
            public boolean check(String action) {
                return predicate.test(action);
            }

            @Override
            public boolean isEmpty() {
                return false;
            }
        }

        static class Globals implements Cluster {

            private final List<Global> globals;

            public Globals(List<Global> globals) {
                this.globals = globals;
            }

            @Override
            public boolean check(String action) {
                if (globals == null) {
                    return false;
                }
                for (Global global : globals) {
                    if (global.cluster().check(action)) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public boolean isEmpty() {
                if (globals == null || globals.isEmpty()) {
                    return true;
                }
                for (Global global : globals) {
                    if (!global.isEmpty()) {
                        return false;
                    }
                }
                return true;
            }
        }

    }

    static interface Indices extends Permission, Iterable<Indices.Group> {
        Map<String, IndicesAccessControl.IndexAccessControl> authorize(String action, Set<String> requestedIndicesOrAliases, MetaData metaData);

        public static class Core implements Indices {

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
            public Map<String, IndicesAccessControl.IndexAccessControl> authorize(String action, Set<String> requestedIndicesOrAliases, MetaData metaData) {
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
                            concreteIndices.add(indexMetaData.getIndex());
                        }
                    }

                    for (Permission.Indices.Group group : groups) {
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

        public static class Globals implements Indices {

            private final List<Global> globals;

            public Globals(List<Global> globals) {
                this.globals = globals;
            }

            @Override
            public Iterator<Group> iterator() {
                return globals == null || globals.isEmpty() ?
                        Collections.<Group>emptyIterator() :
                        new Iter(globals);
            }

            @Override
            public boolean isEmpty() {
                if (globals == null || globals.isEmpty()) {
                    return true;
                }
                for (Global global : globals) {
                    if (!global.indices().isEmpty()) {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public Map<String, IndicesAccessControl.IndexAccessControl> authorize(String action, Set<String> requestedIndicesOrAliases, MetaData metaData) {
                if (isEmpty()) {
                    return emptyMap();
                }

                // What this code does is just merge `IndexAccessControl` instances from the permissions this class holds:
                Map<String, IndicesAccessControl.IndexAccessControl> indicesAccessControl = null;
                for (Global permission : globals) {
                    Map<String, IndicesAccessControl.IndexAccessControl> temp = permission.indices().authorize(action, requestedIndicesOrAliases, metaData);
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

                private final Iterator<Global> globals;
                private Iterator<Group> current;

                Iter(List<Global> globals) {
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
                        Indices indices = globals.next().indices();
                        if (!indices.isEmpty()) {
                            current = indices.iterator();
                            return;
                        }
                    }

                    current = null;
                }
            }
        }

        public static class Group {
            private final Privilege.Index privilege;
            private final Predicate<String> actionMatcher;
            private final String[] indices;
            private final Predicate<String> indexNameMatcher;
            private final List<String> fields;
            private final BytesReference query;

            public Group(Privilege.Index privilege, @Nullable List<String> fields, @Nullable BytesReference query, String... indices) {
                assert indices.length != 0;
                this.privilege = privilege;
                this.actionMatcher = privilege.predicate();
                this.indices = indices;
                this.indexNameMatcher = new AutomatonPredicate(Automatons.patterns(indices));
                this.fields = fields;
                this.query = query;
            }

            public Privilege.Index privilege() {
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

    // FIXME let's split this up, 11 classes before this in a single file that aren't documented and are extremely important
    static interface RunAs extends Permission {

        /**
         * Checks if this permission grants run as to the specified user
         */
        boolean check(String username);

        class Core implements RunAs {

            public static final Core NONE = new Core(Privilege.General.NONE);

            private final Privilege.General privilege;
            private final Predicate<String> predicate;

            public Core(Privilege.General privilege) {
                this.privilege = privilege;
                this.predicate = privilege.predicate();
            }

            @Override
            public boolean check(String username) {
                return predicate.test(username);
            }

            @Override
            public boolean isEmpty() {
                return this == NONE;
            }
        }

        class Globals implements RunAs {
            private final List<Global> globals;

            public Globals(List<Global> globals) {
                this.globals = globals;
            }

            @Override
            public boolean check(String username) {
                if (globals == null) {
                    return false;
                }
                for (Global global : globals) {
                    if (global.runAs().check(username)) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public boolean isEmpty() {
                if (globals == null || globals.isEmpty()) {
                    return true;
                }
                for (Global global : globals) {
                    if (!global.isEmpty()) {
                        return false;
                    }
                }
                return true;
            }
        }
    }
}
