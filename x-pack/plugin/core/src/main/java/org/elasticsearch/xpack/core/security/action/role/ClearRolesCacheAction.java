/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.role;

import org.elasticsearch.action.ActionType;

/**
 * The action for clearing the cache used by native roles that are stored in an index.
 */
public class ClearRolesCacheAction extends ActionType<ClearRolesCacheResponse> {

    public static final ClearRolesCacheAction INSTANCE = new ClearRolesCacheAction();
    public static final String NAME = "cluster:admin/xpack/security/roles/cache/clear";

    protected ClearRolesCacheAction() {
        super(NAME, ClearRolesCacheResponse::new);
    }
}
