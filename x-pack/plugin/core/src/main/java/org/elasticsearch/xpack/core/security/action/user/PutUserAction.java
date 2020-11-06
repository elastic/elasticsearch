/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.action.ActionType;

/**
 * ActionType for putting (adding/updating) a native user.
 */
public class PutUserAction extends ActionType<PutUserResponse> {

    public static final PutUserAction INSTANCE = new PutUserAction();
    public static final String NAME = "cluster:admin/xpack/security/user/put";

    protected PutUserAction() {
        super(NAME, PutUserResponse::new);
    }
}
