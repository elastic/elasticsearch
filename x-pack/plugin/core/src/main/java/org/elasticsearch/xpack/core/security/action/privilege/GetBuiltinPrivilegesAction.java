/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.privilege;

import org.elasticsearch.action.ActionType;

/**
 * ActionType for retrieving builtin privileges from security
 */
public final class GetBuiltinPrivilegesAction extends ActionType<GetBuiltinPrivilegesResponse> {

    public static final GetBuiltinPrivilegesAction INSTANCE = new GetBuiltinPrivilegesAction();
    public static final String NAME = "cluster:admin/xpack/security/privilege/builtin/get";

    private GetBuiltinPrivilegesAction() {
        super(NAME, GetBuiltinPrivilegesResponse::new);
    }
}
