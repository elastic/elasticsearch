/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.action.ActionType;

/**
 * ActionType for cloning an API key. Creates a new API key with the same role descriptors
 * as an existing key (identified by its credential), with a new name, id, and optional expiry.
 * Returns {@link CreateApiKeyResponse} like {@link CreateApiKeyAction} and {@link GrantApiKeyAction}.
 */
public final class CloneApiKeyAction extends ActionType<CreateApiKeyResponse> {

    public static final String NAME = "cluster:admin/xpack/security/api_key/clone";
    public static final CloneApiKeyAction INSTANCE = new CloneApiKeyAction();

    private CloneApiKeyAction() {
        super(NAME);
    }

}
