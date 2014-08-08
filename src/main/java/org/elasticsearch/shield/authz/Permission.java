/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz;

import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.base.Predicate;
import org.elasticsearch.common.collect.Sets;
import org.elasticsearch.shield.support.AutomatonPredicate;
import org.elasticsearch.transport.TransportRequest;

import java.util.Collections;
import java.util.Set;

/**
 *
 */
public abstract class Permission {

    public abstract boolean check(String action, TransportRequest request, MetaData metaData);

    public static Cluster cluster(Privilege.Cluster clusterPrivilege) {
        return new Cluster(clusterPrivilege.predicate());
    }

    public static Index index(Privilege.Index indexPrivilege, String... indexNamePatterns) {
        assert indexNamePatterns.length != 0 : "Index permissions must at least be defined on a single index";

        Automaton indices = new RegExp(indexNamePatterns[0]).toAutomaton();
        for (int i = 1; i < indexNamePatterns.length; i++) {
            indices.union(new RegExp(indexNamePatterns[i]).toAutomaton());
        }
        return new Index(new AutomatonPredicate(indices), indexPrivilege.predicate());
    }

    public static Compound.Builder compound() {
        return new Compound.Builder();
    }

    public static class Index extends Permission {

        private final Predicate<String> indicesMatcher;
        private final Predicate<String> actionMatcher;

        private Index(Predicate<String> indicesMatcher, Predicate<String> actionMatcher) {
            this.indicesMatcher = indicesMatcher;
            this.actionMatcher = actionMatcher;
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
                    return new String[]{"_all"};

                    /* the following is an alternative to requiring explicit privileges for _all, we just expand it, we could potentially extract
                    this code fragment to a separate method in MetaData#concreteIndices in the open source and just use it here]

                    if (indicesRequest.indicesOptions().expandWildcardsOpen() && indicesRequest.indicesOptions().expandWildcardsClosed()) {
                        return metaData.concreteAllIndices();
                    } else if (indicesRequest.indicesOptions().expandWildcardsOpen()) {
                        return metaData.concreteAllOpenIndices();
                    } else {
                        return metaData.concreteAllClosedIndices();
                    }*/

                }
                return metaData.convertFromWildcards(indicesRequest.indices(), indicesRequest.indicesOptions());
            }
            return indicesRequest.indices();
        }
    }

    public static class Cluster extends Permission {

        private final Predicate<String> actionMatcher;

        private Cluster(Predicate<String> actionMatcher) {
            this.actionMatcher = actionMatcher;
        }

        @Override
        public boolean check(String action, TransportRequest request, MetaData metaData) {
            return actionMatcher.apply(action);
        }
    }

    public static class Compound extends Permission {

        private final Permission[] permissions;

        private Compound(Permission... permissions) {
            this.permissions = permissions;
        }

        @Override
        public boolean check(String action, TransportRequest request, MetaData metaData) {
            for (int i = 0; i < permissions.length; i++) {
                if (permissions[i].check(action, request, metaData)) {
                    return true;
                }
            }
            return false;
        }

        public static class Builder {

            private Permission[] permissions = null;

            private Builder() {}

            public void add(Permission... permissions) {
                if (this.permissions == null) {
                    this.permissions = permissions;
                    return;
                }
                Permission[] extended = new Permission[this.permissions.length + permissions.length];
                System.arraycopy(this.permissions, 0, extended, 0, this.permissions.length);
                System.arraycopy(permissions, 0, extended, this.permissions.length, permissions.length);
            }

            public Compound build() {
                return new Compound(permissions);
            }
        }
    }

}
