/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.action.ActionType;

public class ProfileHasPrivilegesAction extends ActionType<ProfileHasPrivilegesResponse> {

    public static final ProfileHasPrivilegesAction INSTANCE = new ProfileHasPrivilegesAction();
    public static final String NAME = "cluster:admin/xpack/security/profile/has_privileges";

    private ProfileHasPrivilegesAction() {
        super(NAME, ProfileHasPrivilegesResponse::new);
    }
}
