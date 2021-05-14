/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.action.ActionType;

/**
 * ActionType for retrieving a user from the security index
 */
public class GetUsersAction extends ActionType<GetUsersResponse> {

    public static final GetUsersAction INSTANCE = new GetUsersAction();
    public static final String NAME = "cluster:admin/xpack/security/user/get";

    protected GetUsersAction() {
        super(NAME, GetUsersResponse::new);
    }
}
