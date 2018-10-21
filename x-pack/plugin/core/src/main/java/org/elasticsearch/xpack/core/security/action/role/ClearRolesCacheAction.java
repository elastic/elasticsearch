/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.role;

import org.elasticsearch.action.Action;

/**
 * The action for clearing the cache used by native roles that are stored in an index.
 */
public class ClearRolesCacheAction extends Action<ClearRolesCacheResponse> {

    public static final ClearRolesCacheAction INSTANCE = new ClearRolesCacheAction();
    public static final String NAME = "cluster:admin/xpack/security/roles/cache/clear";

    protected ClearRolesCacheAction() {
        super(NAME);
    }

    @Override
    public ClearRolesCacheResponse newResponse() {
        return new ClearRolesCacheResponse();
    }
}
