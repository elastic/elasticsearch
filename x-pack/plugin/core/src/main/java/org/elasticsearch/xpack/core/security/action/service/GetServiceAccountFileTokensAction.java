/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.service;

import org.elasticsearch.action.ActionType;

public class GetServiceAccountFileTokensAction extends ActionType<GetServiceAccountFileTokensResponse> {

    public static final String NAME = GetServiceAccountCredentialsAction.NAME + "/file_tokens";
    public static final GetServiceAccountFileTokensAction INSTANCE = new GetServiceAccountFileTokensAction();

    public GetServiceAccountFileTokensAction() { super(NAME, GetServiceAccountFileTokensResponse::new);
    }
}
