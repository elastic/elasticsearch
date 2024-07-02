/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import java.util.Set;

import static org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver.isClusterAction;

/**
 * An {@link ActionClusterPrivilege} that defines access to a raw cluster action name or pattern, instead of a predefined
 * {@link BuiltinClusterPrivilege}.
 */
final class RawActionPatternClusterPrivilege extends ActionClusterPrivilege {
    RawActionPatternClusterPrivilege(String name) {
        super(name, Set.of(actionToPattern(name)));
        assert isClusterAction(name);
    }

    private static String actionToPattern(String text) {
        return text + "*";
    }

    /**
     * Serverless mode does not support specifying raw action patterns for privileges. Only builtin privileges are supported.
     */
    @Override
    public boolean isSupportedInServerlessMode() {
        return false;
    }
}
