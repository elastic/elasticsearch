/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.action.ActionType;

/**
 * ActionType for deleting a native user.
 */
public class DeleteUserAction extends ActionType<DeleteUserResponse> {

    public static final DeleteUserAction INSTANCE = new DeleteUserAction();
    public static final String NAME = "cluster:admin/xpack/security/user/delete";

    protected DeleteUserAction() {
        super(NAME, DeleteUserResponse::new);
    }
}
