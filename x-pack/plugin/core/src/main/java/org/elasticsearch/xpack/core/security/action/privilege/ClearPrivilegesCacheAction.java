/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.privilege;

import org.elasticsearch.action.ActionType;

public class ClearPrivilegesCacheAction extends ActionType<ClearPrivilegesCacheResponse> {

    public static final ClearPrivilegesCacheAction INSTANCE = new ClearPrivilegesCacheAction();
    public static final String NAME = "cluster:admin/xpack/security/privilege/cache/clear";

    protected ClearPrivilegesCacheAction() {
        super(NAME, ClearPrivilegesCacheResponse::new);
    }
}
