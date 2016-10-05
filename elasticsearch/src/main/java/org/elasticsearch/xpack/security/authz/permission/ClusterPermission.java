/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.permission;

import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.security.authc.Authentication;
import org.elasticsearch.xpack.security.authz.privilege.ClusterPrivilege;

import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * A permission that is based on privileges for cluster wide actions
 */
public interface ClusterPermission extends Permission {

    boolean check(String action, TransportRequest request, Authentication authentication);

    class Core implements ClusterPermission {

        public static final Core NONE = new Core(ClusterPrivilege.NONE) {
            @Override
            public boolean check(String action, TransportRequest request, Authentication authentication) {
                return false;
            }

            @Override
            public boolean isEmpty() {
                return true;
            }
        };

        private final ClusterPrivilege privilege;
        private final Predicate<String> predicate;

        Core(ClusterPrivilege privilege) {
            this.privilege = privilege;
            this.predicate = privilege.predicate();
        }

        public ClusterPrivilege privilege() {
            return privilege;
        }

        @Override
        public boolean check(String action, TransportRequest request, Authentication authentication) {
            return predicate.test(action);
        }

        @Override
        public boolean isEmpty() {
            return false;
        }
    }

    class Globals implements ClusterPermission {

        private final List<GlobalPermission> globals;

        Globals(List<GlobalPermission> globals) {
            this.globals = globals;
        }

        @Override
        public boolean check(String action, TransportRequest request, Authentication authentication) {
            if (globals == null) {
                return false;
            }
            for (GlobalPermission global : globals) {
                Objects.requireNonNull(global, "global must not be null");
                Objects.requireNonNull(global.indices(), "global.indices() must not be null");
                if (global.cluster().check(action, request, authentication)) {
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
            for (GlobalPermission global : globals) {
                if (!global.isEmpty()) {
                    return false;
                }
            }
            return true;
        }
    }

}
