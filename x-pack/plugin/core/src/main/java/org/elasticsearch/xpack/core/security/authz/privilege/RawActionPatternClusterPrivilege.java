/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import java.util.Set;

import static org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver.isClusterAction;

final class RawActionPatternClusterPrivilege extends ActionClusterPrivilege {
    public RawActionPatternClusterPrivilege(String name) {
        super(name, Set.of(actionToPattern(name)));
        assert isClusterAction(name);
    }

    private static String actionToPattern(String text) {
        return text + "*";
    }

    @Override
    public boolean isSupportedInServerlessMode() {
        return false;
    }
}
