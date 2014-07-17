/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz;

import com.google.common.base.Predicate;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.shield.support.AutomatonPredicate;
import org.elasticsearch.transport.TransportRequest;

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

            assert request instanceof IndicesRelatedRequest :
                    "the only requests passing the action matcher should be IndexRelatedRequests";

            // if for some reason we missing an action... just for safety we'll reject
            if (!(request instanceof IndicesRelatedRequest)) {
                return false;
            }

            IndicesRelatedRequest req = (IndicesRelatedRequest) request;
            for (String index : req.relatedIndices()) {
                if (!indicesMatcher.apply(index)) {
                    return false;
                }
            }
            return true;
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
