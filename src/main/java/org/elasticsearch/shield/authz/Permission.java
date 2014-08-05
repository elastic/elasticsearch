/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz;

import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.base.Predicate;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.collect.Sets;
import org.elasticsearch.shield.support.AutomatonPredicate;
import org.elasticsearch.shield.support.Automatons;
import org.elasticsearch.transport.TransportRequest;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

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

    boolean check(String action, TransportRequest request, MetaData metaData);

    public static class Global implements Permission {

        private final Cluster cluster;
        private final Indices indices;

        Global() {
            this(null, null);
        }

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

        public boolean check(String action, TransportRequest request, MetaData metaData) {
            if (cluster != null && cluster.check(action, request, metaData)) {
                return true;
            }
            if (indices != null && indices.check(action, request, metaData)) {
                return true;
            }
            return false;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {

            private Cluster cluster = Cluster.NONE;
            private ImmutableList.Builder<Indices.Group> groups;

            private Builder() {
            }

            public Builder set(Privilege.Cluster privilege) {
                cluster = new Cluster(privilege);
                return this;
            }

            public Builder add(Privilege.Index privilege, String... indices) {
                if (groups == null) {
                    groups = ImmutableList.builder();
                }
                groups.add(new Indices.Group(privilege, indices));
                return this;
            }

            public Global build() {
                Indices indices = groups != null ? new Indices(groups.build()) : Indices.NONE;
                return new Global(cluster, indices);
            }
        }
    }

    public static class Cluster implements Permission {

        public static final Cluster NONE = new Cluster(Privilege.Cluster.NONE) {
            @Override
            public boolean check(String action, TransportRequest request, MetaData metaData) {
                return false;
            }
        };

        private final Privilege.Cluster privilege;
        private final Predicate<String> predicate;

        private Cluster(Privilege.Cluster privilege) {
            this.privilege = privilege;
            this.predicate = privilege.predicate();
        }

        public Privilege.Cluster privilege() {
            return privilege;
        }

        @Override
        public boolean check(String action, TransportRequest request, MetaData metaData) {
            return predicate.apply(action);
        }
    }

    public static class Indices implements Permission {

        public static final Indices NONE = new Indices() {
            @Override
            public boolean check(String action, TransportRequest request, MetaData metaData) {
                return false;
            }
        };

        private Group[] groups;

        public Indices(Collection<Group> groups) {
            this(groups.toArray(new Group[groups.size()]));
        }

        public Indices(Group... groups) {
            this.groups = groups;
        }

        public Group[] groups() {
            return groups;
        }

        /**
         * @return  A predicate that will match all the indices that this permission
         *          has the given privilege for.
         */
        public Predicate<String> allowedIndicesMatcher(Privilege.Index privilege) {
            ImmutableList.Builder<String> indices = ImmutableList.builder();
            for (Group group : groups) {
                if (group.privilege.implies(privilege)) {
                    indices.add(group.indices);
                }
            }
            return new AutomatonPredicate(Automatons.patterns(indices.build()));
        }

        /**
         * @return  A predicate that will match all the indices that this permission
         *          has the privilege for executing the given action on.
         */
        public Predicate<String> allowedIndicesMatcher(String action) {
            ImmutableList.Builder<String> indices = ImmutableList.builder();
            for (Group group : groups) {
                if (group.actionMatcher.apply(action)) {
                    indices.add(group.indices);
                }
            }
            return new AutomatonPredicate(Automatons.patterns(indices.build()));
        }

        @Override
        public boolean check(String action, TransportRequest request, MetaData metaData) {
            for (int i = 0; i < groups.length; i++) {
                if (groups[i].check(action, request, metaData)) {
                    return true;
                }
            }
            return false;
        }

        public static class Group implements Permission {

            private final Privilege.Index privilege;
            private final Predicate<String> actionMatcher;
            private final String[] indices;
            private final Predicate<String> indicesMatcher;

            public Group(Privilege.Index privilege, String... indices) {
                assert indices.length != 0;
                this.privilege = privilege;
                this.actionMatcher = privilege.predicate();
                this.indices = indices;
                this.indicesMatcher = new AutomatonPredicate(Automatons.patterns(indices));
            }

            public Privilege.Index privilege() {
                return privilege;
            }

            public String[] indices() {
                return indices;
            }

            @Override
            public boolean check(String action, TransportRequest request, MetaData metaData) {

                if (!actionMatcher.apply(action)) {
                    return false;
                }

                boolean isIndicesRequest = request instanceof CompositeIndicesRequest || request instanceof IndicesRequest;

                assert isIndicesRequest : "the only requests passing the action matcher should be IndicesRequests";

                // if for some reason we are missing an action... just for safety we'll reject
                if (!isIndicesRequest) {
                    return false;
                }

                Set<String> indices = Sets.newHashSet();
                if (request instanceof CompositeIndicesRequest) {
                    CompositeIndicesRequest compositeIndicesRequest = (CompositeIndicesRequest) request;
                    for (IndicesRequest indicesRequest : compositeIndicesRequest.subRequests()) {
                        Collections.addAll(indices, explodeWildcards(indicesRequest, metaData));
                    }
                } else {
                    Collections.addAll(indices, explodeWildcards((IndicesRequest) request, metaData));
                }

                for (String index : indices) {
                    if (!indicesMatcher.apply(index)) {
                        return false;
                    }
                }
                return true;
            }

            private String[] explodeWildcards(IndicesRequest indicesRequest, MetaData metaData) {
                if (indicesRequest.indicesOptions().expandWildcardsOpen() || indicesRequest.indicesOptions().expandWildcardsClosed()) {
                    if (MetaData.isAllIndices(indicesRequest.indices())) {
                        if (indicesRequest.indicesOptions().expandWildcardsOpen() && indicesRequest.indicesOptions().expandWildcardsClosed()) {
                            return metaData.concreteAllIndices();
                        }
                        if (indicesRequest.indicesOptions().expandWildcardsOpen()) {
                            return metaData.concreteAllOpenIndices();
                        }
                        return metaData.concreteAllClosedIndices();

                    }
                    return metaData.convertFromWildcards(indicesRequest.indices(), indicesRequest.indicesOptions());
                }
                return indicesRequest.indices();
            }
        }
    }

}
