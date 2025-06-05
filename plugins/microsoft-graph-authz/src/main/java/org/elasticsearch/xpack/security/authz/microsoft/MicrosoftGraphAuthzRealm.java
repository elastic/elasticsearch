/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xpack.security.authz.microsoft;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.user.User;

public class MicrosoftGraphAuthzRealm extends Realm {

    private static final Logger logger = LogManager.getLogger(MicrosoftGraphAuthzRealm.class);

    public MicrosoftGraphAuthzRealm(RealmConfig config) {
        super(config);
    }

    @Override
    public boolean supports(AuthenticationToken token) {
        return false;
    }

    @Override
    public AuthenticationToken token(ThreadContext context) {
        return null;
    }

    @Override
    public void authenticate(AuthenticationToken token, ActionListener<AuthenticationResult<User>> listener) {
        listener.onResponse(AuthenticationResult.notHandled());
    }

    @Override
    public void lookupUser(String username, ActionListener<User> listener) {
        logger.info("Microsoft Graph Authz not yet implemented, returning empty roles for [{}]", username);
        listener.onResponse(new User(username));
    }
}
