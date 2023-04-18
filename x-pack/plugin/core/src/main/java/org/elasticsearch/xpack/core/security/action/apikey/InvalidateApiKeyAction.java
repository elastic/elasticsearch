/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.action.ActionType;

/**
 * ActionType for invalidating API key
 */
public final class InvalidateApiKeyAction extends ActionType<InvalidateApiKeyResponse> {

    public static final String NAME = "cluster:admin/xpack/security/api_key/invalidate";
    public static final InvalidateApiKeyAction INSTANCE = new InvalidateApiKeyAction();

    private InvalidateApiKeyAction() {
        super(NAME, InvalidateApiKeyResponse::new);
    }
}
