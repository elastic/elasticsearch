/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.action.ActionType;

/**
 * ActionType that lists the set of privileges held by a user.
 */
public final class GetUserPrivilegesAction extends ActionType<GetUserPrivilegesResponse> {

    public static final GetUserPrivilegesAction INSTANCE = new GetUserPrivilegesAction();
    public static final String NAME = "cluster:admin/xpack/security/user/list_privileges";

    private GetUserPrivilegesAction() {
        super(NAME, GetUserPrivilegesResponse::new);
    }
}
