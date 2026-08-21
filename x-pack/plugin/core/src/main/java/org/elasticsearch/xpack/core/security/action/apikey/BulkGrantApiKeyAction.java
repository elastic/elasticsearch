/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.action.ActionType;

/**
 * ActionType for creating multiple API keys on behalf of another user in a single request.
 * Nested under {@link GrantApiKeyAction#NAME} so the existing {@code grant_api_key} cluster privilege
 * authorizes this action without a separate privilege.
 */
public final class BulkGrantApiKeyAction extends ActionType<BulkGrantApiKeyResponse> {

    public static final String NAME = GrantApiKeyAction.NAME + "/bulk";
    public static final BulkGrantApiKeyAction INSTANCE = new BulkGrantApiKeyAction();

    private BulkGrantApiKeyAction() {
        super(NAME);
    }
}
