/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.oauth2;

import org.elasticsearch.action.ActionListener;

/**
 * Fetches a fresh OAuth2 bearer token asynchronously. Implementations capture the
 * endpoint-specific configuration (token URL, client ID, client secret, scopes).
 *
 * <p>Mirrors the shape of {@code OAuth2TokenSupplier} in
 * <a href="https://github.com/elastic/elasticsearch/pull/149217">#149217</a> so
 * that PR's cache implementation can drop in without touching consumers.
 */
@FunctionalInterface
public interface OAuth2TokenSupplier {
    void fetch(ActionListener<CachedToken> listener);
}
