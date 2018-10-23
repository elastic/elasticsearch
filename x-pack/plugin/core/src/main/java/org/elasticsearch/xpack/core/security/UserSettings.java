/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;

public final class UserSettings {
    private final Logger logger = LogManager.getLogger(UserSettings.class);

    private final ThreadContext threadContext;

    UserSettings(ThreadContext threadContext) {
        this.threadContext = threadContext;
    }

    /**
     * Returns the current user information, or null if the current request has no authentication info.
     */
    public User getUser() {
        Authentication authentication = getAuthentication();
        return authentication == null ? null : authentication.getUser();
    }

    /**
     * Returns the authentication information, or null if the current request has no authentication info.
     */
    public Authentication getAuthentication() {
        try {
            return Authentication.readFromContext(threadContext);
        } catch (IOException e) {
            // TODO: this seems bogus, the only way to get an ioexception here is from a corrupt or tampered
            // auth header, which should be be audited?
            logger.error("failed to read authentication", e);
            return null;
        }
    }
}
