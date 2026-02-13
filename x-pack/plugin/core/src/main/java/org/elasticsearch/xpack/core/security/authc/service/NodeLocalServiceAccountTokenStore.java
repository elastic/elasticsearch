/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc.service;

import org.elasticsearch.xpack.core.security.action.service.TokenInfo;

import java.util.List;

public interface NodeLocalServiceAccountTokenStore extends ServiceAccountTokenStore {
    default List<TokenInfo> findNodeLocalTokensFor(ServiceAccount.ServiceAccountId accountId) {
        throw new IllegalStateException("Find node local tokens not supported by [" + this.getClass() + "]");
    }
}
