/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.token;

import org.elasticsearch.action.ActionType;

/**
 * ActionType for invalidating one or more tokens
 */
public final class InvalidateTokenAction extends ActionType<InvalidateTokenResponse> {

    public static final String NAME = "cluster:admin/xpack/security/token/invalidate";
    public static final InvalidateTokenAction INSTANCE = new InvalidateTokenAction();

    private InvalidateTokenAction() {
        super(NAME);
    }
}
