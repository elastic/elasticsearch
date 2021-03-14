/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.encrypted.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.repositories.verify.VerifyRepositoryResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;

public class ChangeEncryptedRepositoryPasswordAction extends ActionType<EncryptedRepositoryChangePasswordResponse> {

    public static final String NAME = "cluster:admin/repository/change_password";

    public static final ChangeEncryptedRepositoryPasswordAction INSTANCE = new ChangeEncryptedRepositoryPasswordAction();

    private ChangeEncryptedRepositoryPasswordAction() {
        super(NAME, EncryptedRepositoryChangePasswordResponse::new);
    }
}
