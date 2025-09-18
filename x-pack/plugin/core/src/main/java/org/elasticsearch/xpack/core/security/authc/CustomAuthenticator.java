/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;

/**
 * An extension point to provide a custom authenticator implementation. For example, a custom API key or a custom OAuth2
 * token implementation. The implementation is wrapped by a core `Authenticator` class and included in the authenticator chain
 * _before_ the respective "standard" authenticator(s).
 */
public interface CustomAuthenticator {

    boolean supports(AuthenticationToken token);

    @Nullable
    AuthenticationToken extractToken(ThreadContext context);

    void authenticate(@Nullable AuthenticationToken token, ActionListener<AuthenticationResult<Authentication>> listener);

}
