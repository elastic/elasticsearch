/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.action.ActionType;

public final class BulkUpdateApiKeyAction extends ActionType<BulkUpdateApiKeyResponse> {

    public static final String NAME = "cluster:admin/xpack/security/api_key/bulk_update";
    public static final BulkUpdateApiKeyAction INSTANCE = new BulkUpdateApiKeyAction();

    private BulkUpdateApiKeyAction() {
        super(NAME, BulkUpdateApiKeyResponse::new);
    }
}
