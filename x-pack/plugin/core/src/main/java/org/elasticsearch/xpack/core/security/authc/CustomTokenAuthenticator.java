/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.security.action.Grant;

/**
 * Represents a custom authenticator that supports access token authentication method.
 */
public interface CustomTokenAuthenticator extends CustomAuthenticator {

    /**
     * Called to extract {@code AuthenticationToken} for the {@link Grant#ACCESS_TOKEN_GRANT_TYPE}.
     *
     * <p>
     * This method is called to extract an access token during:
     * <ul>
     *   <li>User profile activation - the extracted token is used to authenticate the user before creating a user profile</li>
     *   <li>Grant API key - the extracted token is used to authenticate the user on whose behalf the API key is being created</li>
     * </ul>
     *
     * <p>
     * The extracted token is then used to call the {@link #authenticate(AuthenticationToken, ActionListener)} method.
     *
     * <p>
     * To opt-out, implementors should return {@code null} if using token for grant endpoints is not supported.
     *
     * @param grant grant that holds end-user credentials
     * @return an authentication token if grant holds credentials
     *        that are supported by this authenticator
     */
    @Nullable
    default AuthenticationToken extractGrantAccessToken(Grant grant) {
        return null;
    }
}
