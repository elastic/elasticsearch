/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.enrollment;

import org.elasticsearch.action.ActionType;

/**
 * ActionType for creating an enrollment new token
 */
public class CreateEnrollmentTokenAction extends ActionType<CreateEnrollmentTokenResponse> {
    public static final String NAME = "cluster:admin/xpack/security/enrollment/create";
    public static final org.elasticsearch.xpack.core.security.action.enrollment.CreateEnrollmentTokenAction INSTANCE =
        new CreateEnrollmentTokenAction();

    private CreateEnrollmentTokenAction() {
        super(NAME, CreateEnrollmentTokenResponse::new);
    }
}
