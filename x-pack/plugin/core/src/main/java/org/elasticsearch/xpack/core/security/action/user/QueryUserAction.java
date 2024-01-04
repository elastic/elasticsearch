/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.io.stream.Writeable;

/**
 * ActionType for retrieving Users
 */
public final class QueryUserAction extends ActionType<QueryUserResponse> {

    public static final String NAME = "cluster:admin/xpack/security/user/query";
    public static final QueryUserAction INSTANCE = new QueryUserAction();

    private QueryUserAction() {
        super(NAME, Writeable.Reader.localOnly());
    }
}
