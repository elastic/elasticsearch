/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.service;

import org.elasticsearch.action.ActionType;

public class DeleteServiceAccountTokenAction extends ActionType<DeleteServiceAccountTokenResponse> {

    public static final String NAME = "cluster:admin/xpack/security/service_account/token/delete";
    public static final DeleteServiceAccountTokenAction INSTANCE = new DeleteServiceAccountTokenAction();

    public DeleteServiceAccountTokenAction() {
        super(NAME, DeleteServiceAccountTokenResponse::new);
    }
}
