/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.privilege;

import org.elasticsearch.action.ActionType;

/**
 * ActionType for putting (adding/updating) one or more application privileges.
 */
public final class PutPrivilegesAction extends ActionType<PutPrivilegesResponse> {

    public static final PutPrivilegesAction INSTANCE = new PutPrivilegesAction();
    public static final String NAME = "cluster:admin/xpack/security/privilege/put";

    private PutPrivilegesAction() {
        super(NAME, PutPrivilegesResponse::new);
    }
}
