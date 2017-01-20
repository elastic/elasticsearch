/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.ThreadContext.StoredContext;
import org.elasticsearch.node.Node;
import org.elasticsearch.xpack.security.authc.Authentication;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.crypto.CryptoService;
import org.elasticsearch.xpack.security.user.User;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * A lightweight utility that can find the current user and authentication information for the local thread.
 */
public class SecurityContext {

    private final Logger logger;
    private final ThreadContext threadContext;
    private final CryptoService cryptoService;
    private final boolean signUserHeader;
    private final String nodeName;

    /**
     * Creates a new security context.
     * If cryptoService is null, security is disabled and {@link #getUser()}
     * and {@link #getAuthentication()} will always return null.
     */
    public SecurityContext(Settings settings, ThreadContext threadContext, CryptoService cryptoService) {
        this.logger = Loggers.getLogger(getClass(), settings);
        this.threadContext = threadContext;
        this.cryptoService = cryptoService;
        this.signUserHeader = AuthenticationService.SIGN_USER_HEADER.get(settings);
        this.nodeName = Node.NODE_NAME_SETTING.get(settings);
    }

    /** Returns the current user information, or null if the current request has no authentication info. */
    public User getUser() {
        Authentication authentication = getAuthentication();
        return authentication == null ? null : authentication.getUser();
    }

    /** Returns the authentication information, or null if the current request has no authentication info. */
    public Authentication getAuthentication() {
        try {
            return Authentication.readFromContext(threadContext, cryptoService, signUserHeader);
        } catch (IOException e) {
            // TODO: this seems bogus, the only way to get an ioexception here is from a corrupt or tampered
            // auth header, which should be be audited?
            logger.error("failed to read authentication", e);
            return null;
        }
    }

    /**
     * Sets the user forcefully to the provided user. There must not be an existing user in the ThreadContext otherwise an exception
     * will be thrown. This method is package private for testing.
     */
    void setUser(User user) {
        Objects.requireNonNull(user);
        final Authentication.RealmRef lookedUpBy;
        if (user.runAs() == null) {
            lookedUpBy = null;
        } else {
            lookedUpBy = new Authentication.RealmRef("__attach", "__attach", nodeName);
        }

        try {
            Authentication authentication =
                    new Authentication(user, new Authentication.RealmRef("__attach", "__attach", nodeName), lookedUpBy);
            authentication.writeToContext(threadContext, cryptoService, signUserHeader);
        } catch (IOException e) {
            throw new AssertionError("how can we have a IOException with a user we set", e);
        }
    }

    /**
     * Runs the consumer in a new context as the provided user. The original constext is provided to the consumer. When this method
     * returns, the original context is restored.
     */
    public void executeAsUser(User user, Consumer<StoredContext> consumer) {
        final StoredContext original = threadContext.newStoredContext(true);
        try (ThreadContext.StoredContext ctx = threadContext.stashContext()) {
            setUser(user);
            consumer.accept(original);
        }
    }
}
