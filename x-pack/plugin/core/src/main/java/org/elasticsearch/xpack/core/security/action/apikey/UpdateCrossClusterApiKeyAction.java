/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.action.ActionType;

public final class UpdateCrossClusterApiKeyAction extends ActionType<UpdateApiKeyResponse> {

    public static final String NAME = "cluster:admin/xpack/security/cross_cluster/api_key/update";
    public static final UpdateCrossClusterApiKeyAction INSTANCE = new UpdateCrossClusterApiKeyAction();

    private UpdateCrossClusterApiKeyAction() {
        super(NAME, UpdateApiKeyResponse::new);
    }
}
