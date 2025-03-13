/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.action.ActionType;

/**
 * ActionType for retrieving API key(s)
 */
public final class GetApiKeyAction extends ActionType<GetApiKeyResponse> {

    public static final String NAME = "cluster:admin/xpack/security/api_key/get";
    public static final GetApiKeyAction INSTANCE = new GetApiKeyAction();

    private GetApiKeyAction() {
        super(NAME);
    }
}
