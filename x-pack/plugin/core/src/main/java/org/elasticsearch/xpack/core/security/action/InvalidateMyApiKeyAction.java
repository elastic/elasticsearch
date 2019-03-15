/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.common.io.stream.Writeable;

/**
 * Action for invalidating API key for the authenticated user
 */
public final class InvalidateMyApiKeyAction extends Action<InvalidateApiKeyResponse> {

    public static final String NAME = "cluster:admin/xpack/security/api_key/invalidate/my";
    public static final InvalidateMyApiKeyAction INSTANCE = new InvalidateMyApiKeyAction();

    private InvalidateMyApiKeyAction() {
        super(NAME);
    }

    @Override
    public InvalidateApiKeyResponse newResponse() {
        throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
    }

    @Override
    public Writeable.Reader<InvalidateApiKeyResponse> getResponseReader() {
        return InvalidateApiKeyResponse::new;
    }
}
