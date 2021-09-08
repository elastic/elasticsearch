/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.realm;

import org.elasticsearch.action.ActionType;

public class ClearRealmCacheAction extends ActionType<ClearRealmCacheResponse> {

    public static final ClearRealmCacheAction INSTANCE = new ClearRealmCacheAction();
    public static final String NAME = "cluster:admin/xpack/security/realm/cache/clear";

    protected ClearRealmCacheAction() {
        super(NAME, ClearRealmCacheResponse::new);
    }
}
