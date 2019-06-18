/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.privilege;

import org.elasticsearch.action.Action;

/**
 * Action for retrieving one or more application privileges from the security index
 */
public final class GetBuiltinPrivilegesAction extends Action<GetBuiltinPrivilegesResponse> {

    public static final GetBuiltinPrivilegesAction INSTANCE = new GetBuiltinPrivilegesAction();
    public static final String NAME = "cluster:admin/xpack/security/privilege/builtin/get";

    private GetBuiltinPrivilegesAction() {
        super(NAME);
    }

    @Override
    public GetBuiltinPrivilegesResponse newResponse() {
        return new GetBuiltinPrivilegesResponse();
    }
}
