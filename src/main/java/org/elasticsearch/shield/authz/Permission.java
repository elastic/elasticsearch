/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz;

import org.elasticsearch.common.base.Predicate;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.cache.CacheLoader;
import org.elasticsearch.common.cache.LoadingCache;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.collect.UnmodifiableIterator;
import org.elasticsearch.shield.support.AutomatonPredicate;
import org.elasticsearch.shield.support.Automatons;

import java.util.Collections;
import java.util.Iterator;

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
 *     <li>
 *         Global -     a composite permission that combines a both cluster & indices permissions
 *     </li>
 * </ul>
 */
public interface Permission {

    boolean isEmpty();

    static class Global implements Permission {

        public static final Global NONE = new Global(Cluster.Core.NONE, Indices.Core.NONE);

        private final Cluster cluster;
        private final Indices indices;

        Global(Cluster cluster, Indices indices) {
            this.cluster = cluster;
            this.indices = indices;
        }

        public Cluster cluster() {
            return cluster;
        }

        public Indices indices() {
            return indices;
        }

        @Override
        public boolean isEmpty() {
            return (cluster == null || cluster.isEmpty()) && (indices == null || indices.isEmpty());
        }

        public static class Role extends Global {

            private final String name;

            private Role(String name, Cluster.Core cluster, Indices.Core indices) {
                super(cluster, indices);
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

            public static Builder builder(String name) {
                return new Builder(name);
            }

            public static class Builder {

                private final String name;
                private Cluster.Core cluster = Cluster.Core.NONE;
                private ImmutableList.Builder<Indices.Group> groups = ImmutableList.builder();

                private Builder(String name) {
                    this.name = name;
                }

                public Builder set(Privilege.Cluster privilege) {
                    cluster = new Cluster.Core(privilege);
                    return this;
                }

                public Builder add(Privilege.Index privilege, String... indices) {
                    groups.add(new Indices.Group(privilege, indices));
                    return this;
                }

                public Role build() {
                    ImmutableList<Indices.Group> list = groups.build();
                    Indices.Core indices = list.isEmpty() ? Indices.Core.NONE : new Indices.Core(list.toArray(new Indices.Group[list.size()]));
                    return new Role(name, cluster, indices);
                }
            }
        }

        static class Compound extends Global {

            public Compound(ImmutableList<Global> globals) {
                super(new Cluster.Globals(globals), new Indices.Globals(globals));
            }

            public static Builder builder() {
                return new Builder();
            }

            public static class Builder {

                private ImmutableList.Builder<Global> globals = ImmutableList.builder();

                private Builder() {
                }

                public Builder add(Global global) {
                    globals.add(global);
                    return this;
                }

                public Compound build() {
                    return new Compound(globals.build());
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

            public boolean check(String action) {
                return predicate.apply(action);
            }

            @Override
            public boolean isEmpty() {
                return false;
            }
        }

        static class Globals implements Cluster {

            private final ImmutableList<Global> globals;

            public Globals(ImmutableList<Global> globals) {
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

            private final LoadingCache<String, Predicate<String>> allowedIndicesMatchersForAction = CacheBuilder.newBuilder()
                    .build(new CacheLoader<String, Predicate<String>>() {
                        @Override
                        public Predicate<String> load(String action) throws Exception {
                            ImmutableList.Builder<String> indices = ImmutableList.builder();
                            for (Group group : groups) {
                                if (group.actionMatcher.apply(action)) {
                                    indices.add(group.indices);
                                }
                            }
                            return new AutomatonPredicate(Automatons.patterns(indices.build()));
                        }
                    });

            private final Group[] groups;

            public Core(Group... groups) {
                this.groups = groups;
            }

            @Override
            public Iterator<Group> iterator() {
                return Iterators.forArray(groups);
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
                return allowedIndicesMatchersForAction.getUnchecked(action);
            }
        }

        public static class Globals implements Indices {

            private final ImmutableList<Global> globals;

            public Globals(ImmutableList<Global> globals) {
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

            static class Iter extends UnmodifiableIterator<Group> {

                private final Iterator<Global> globals;
                private Iterator<Group> current;

                Iter(ImmutableList<Global> globals) {
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

                private void advance() {
                    if (current != null && current.hasNext()) {
                        return;
                    }
                    if (!globals.hasNext()) {
                        // we've reached the end of the globals array
                        current = null;
                        return;
                    }
                    current = globals.next().indices().iterator();
                }
            }
        }

        public static class Group {
            private final Privilege.Index privilege;
            private final Predicate<String> actionMatcher;
            private final String[] indices;
            private final Predicate<String> indexNameMatcher;

            public Group(Privilege.Index privilege, String... indices) {
                assert indices.length != 0;
                this.privilege = privilege;
                this.actionMatcher = privilege.predicate();
                this.indices = indices;
                this.indexNameMatcher = new AutomatonPredicate(Automatons.patterns(indices));
            }

            public Privilege.Index privilege() {
                return privilege;
            }

            public String[] indices() {
                return indices;
            }

            public boolean check(String action, String index) {
                assert index != null;
                return actionMatcher.apply(action) && indexNameMatcher.apply(index);
            }
        }
    }

}
