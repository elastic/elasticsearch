/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.enrollment;

import org.elasticsearch.action.ActionType;

public final class ClientEnrollmentAction extends ActionType<ClientEnrollmentResponse> {

    public static final String NAME = "cluster:admin/xpack/enroll/client";
    public static final ClientEnrollmentAction INSTANCE = new ClientEnrollmentAction();

    private ClientEnrollmentAction() {
        super(NAME, ClientEnrollmentResponse::new);
    }
}
