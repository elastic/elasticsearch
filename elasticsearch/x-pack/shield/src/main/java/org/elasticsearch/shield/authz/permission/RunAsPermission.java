/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz.permission;

import org.elasticsearch.shield.authz.privilege.GeneralPrivilege;

import java.util.List;
import java.util.function.Predicate;

/**
 * A permissions that is based on a general privilege that contains patterns of users that this
 * user can execute a request as
 */
public interface RunAsPermission extends Permission {

    /**
     * Checks if this permission grants run as to the specified user
     */
    boolean check(String username);

    class Core implements RunAsPermission {

        public static final Core NONE = new Core(GeneralPrivilege.NONE);

        private final GeneralPrivilege privilege;
        private final Predicate<String> predicate;

        public Core(GeneralPrivilege privilege) {
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

    class Globals implements RunAsPermission {
        private final List<GlobalPermission> globals;

        public Globals(List<GlobalPermission> globals) {
            this.globals = globals;
        }

        @Override
        public boolean check(String username) {
            if (globals == null) {
                return false;
            }
            for (GlobalPermission global : globals) {
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
            for (GlobalPermission global : globals) {
                if (!global.isEmpty()) {
                    return false;
                }
            }
            return true;
        }
    }
}
