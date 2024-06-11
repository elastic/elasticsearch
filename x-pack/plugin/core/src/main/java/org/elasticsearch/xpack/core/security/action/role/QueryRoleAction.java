/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.role;

import org.elasticsearch.action.ActionType;

public final class QueryRoleAction extends ActionType<QueryRoleResponse> {

    public static final String NAME = "cluster:admin/xpack/security/role/query";
    public static final QueryRoleAction INSTANCE = new QueryRoleAction();

    public QueryRoleAction() {
        super(NAME);
    }
}
