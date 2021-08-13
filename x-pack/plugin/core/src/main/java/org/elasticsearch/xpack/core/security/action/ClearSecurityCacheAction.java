/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action;

import org.elasticsearch.action.ActionType;

public class ClearSecurityCacheAction extends ActionType<ClearSecurityCacheResponse> {

    public static final ClearSecurityCacheAction INSTANCE = new ClearSecurityCacheAction();
    public static final String NAME = "cluster:admin/xpack/security/cache/clear";

    protected ClearSecurityCacheAction() {
        super(NAME, ClearSecurityCacheResponse::new);
    }
}
