/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.enrollment;

import org.elasticsearch.action.ActionType;

/**
 * ActionType for creating an enrollment new token
 */
public class CreateEnrollmentTokenAction extends ActionType<CreateEnrollmentTokenResponse> {
    public static final String NAME = "cluster:admin/xpack/enrollment/create";
    public static final CreateEnrollmentTokenAction INSTANCE =
        new CreateEnrollmentTokenAction();

    private CreateEnrollmentTokenAction() {
        super(NAME, CreateEnrollmentTokenResponse::new);
    }
}
