/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

/**
 * Request object to disable a native realm or built-in user.
 */
public final class DisableUserRequest extends SetUserEnabledRequest {

    public DisableUserRequest(String username, RefreshPolicy refreshPolicy) {
        super(false, username, refreshPolicy);
    }
}
