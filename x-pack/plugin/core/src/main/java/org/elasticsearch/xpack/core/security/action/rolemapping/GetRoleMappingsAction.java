/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.rolemapping;

import org.elasticsearch.action.ActionType;

/**
 * ActionType to retrieve one or more role-mappings from X-Pack security
 *
 * see org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore
 */
public class GetRoleMappingsAction extends ActionType<GetRoleMappingsResponse> {

    public static final GetRoleMappingsAction INSTANCE = new GetRoleMappingsAction();
    public static final String NAME = "cluster:admin/xpack/security/role_mapping/get";

    private GetRoleMappingsAction() {
        super(NAME, GetRoleMappingsResponse::new);
    }
}
