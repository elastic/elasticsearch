/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.service;

import org.elasticsearch.action.ActionType;

/**
 * Searches the user-managed service accounts. Named under {@code service_account/} rather than
 * {@code user_managed_service_account/} because it only reads: {@code manage_service_account} grants
 * {@link GetServiceAccountAction}, which reports user-managed accounts, and querying them needs no more privilege
 * than getting them.
 */
public class QueryServiceAccountAction extends ActionType<QueryServiceAccountResponse> {

    public static final String NAME = "cluster:admin/xpack/security/service_account/query";
    public static final QueryServiceAccountAction INSTANCE = new QueryServiceAccountAction();

    public QueryServiceAccountAction() {
        super(NAME);
    }
}
