/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.ThreadContext.StoredContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.node.Node;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.AuthenticationType;
import org.elasticsearch.xpack.core.security.authc.support.AuthenticationContextSerializer;
import org.elasticsearch.xpack.core.security.authc.support.SecondaryAuthentication;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.elasticsearch.xpack.core.security.authc.Authentication.VERSION_API_KEY_ROLES_AS_BYTES;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.API_KEY_ROLE_DESCRIPTORS_KEY;

/**
 * A lightweight utility that can find the current user and authentication information for the local thread.
 */
public class SecurityContext {

    private final Logger logger = LogManager.getLogger(SecurityContext.class);

    private final ThreadContext threadContext;
    private final AuthenticationContextSerializer authenticationSerializer;
    private final String nodeName;

    public SecurityContext(Settings settings, ThreadContext threadContext) {
        this.threadContext = threadContext;
        this.authenticationSerializer = new AuthenticationContextSerializer();
        this.nodeName = Node.NODE_NAME_SETTING.get(settings);
    }

    /**
     * Returns the current user information, or throws {@link org.elasticsearch.ElasticsearchSecurityException}
     * if the current request has no authentication information.
     */
    public User requireUser() {
        User user = getUser();
        if (user == null) {
            throw new ElasticsearchSecurityException("there is no user available in the current context");
        }
        return user;
    }

    /** Returns the current user information, or null if the current request has no authentication info. */
    @Nullable
    public User getUser() {
        Authentication authentication = getAuthentication();
        return authentication == null ? null : authentication.getUser();
    }

    /** Returns the authentication information, or null if the current request has no authentication info. */
    @Nullable
    public Authentication getAuthentication() {
        try {
            return authenticationSerializer.readFromContext(threadContext);
        } catch (IOException e) {
            logger.error("failed to read authentication", e);
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Returns the "secondary authentication" (see {@link SecondaryAuthentication}) information,
     * or {@code null} if the current request does not have a secondary authentication context
     */
    public SecondaryAuthentication getSecondaryAuthentication() {
        try {
            return SecondaryAuthentication.readFromContext(this);
        } catch (IOException e) {
            logger.error("failed to read secondary authentication", e);
            throw new UncheckedIOException(e);
        }
    }

    public ThreadContext getThreadContext() {
        return threadContext;
    }

    /**
     * Sets the user forcefully to the provided user. There must not be an existing user in the ThreadContext otherwise an exception
     * will be thrown. This method is package private for testing.
     */
    public void setUser(User user, Version version) {
        Objects.requireNonNull(user);
        final Authentication.RealmRef authenticatedBy = new Authentication.RealmRef("__attach", "__attach", nodeName);
        final Authentication.RealmRef lookedUpBy;
        if (user.isRunAs()) {
            lookedUpBy = authenticatedBy;
        } else {
            lookedUpBy = null;
        }
        setAuthentication(
            new Authentication(user, authenticatedBy, lookedUpBy, version, AuthenticationType.INTERNAL, Collections.emptyMap()));
    }

    /** Writes the authentication to the thread context */
    private void setAuthentication(Authentication authentication) {
        try {
            authentication.writeToContext(threadContext);
        } catch (IOException e) {
            throw new AssertionError("how can we have a IOException with a user we set", e);
        }
    }

    /**
     * Runs the consumer in a new context as the provided user. The original context is provided to the consumer. When this method
     * returns, the original context is restored.
     */
    public void executeAsUser(User user, Consumer<StoredContext> consumer, Version version) {
        final StoredContext original = threadContext.newStoredContext(true);
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            setUser(user, version);
            consumer.accept(original);
        }
    }

    /**
     * Runs the consumer in a new context as the provided user. The original context is provided to the consumer. When this method
     * returns, the original context is restored.
     */
    public <T> T executeWithAuthentication(Authentication authentication, Function<StoredContext, T> consumer) {
        final StoredContext original = threadContext.newStoredContext(true);
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            setAuthentication(authentication);
            return consumer.apply(original);
        }
    }

    /**
     * Runs the consumer in a new context after setting a new version of the authentication that is compatible with the version provided.
     * The original context is provided to the consumer. When this method returns, the original context is restored.
     */
    public void executeAfterRewritingAuthentication(Consumer<StoredContext> consumer, Version version) {
        final StoredContext original = threadContext.newStoredContext(true);
        final Authentication authentication = getAuthentication();
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            setAuthentication(new Authentication(authentication.getUser(), authentication.getAuthenticatedBy(),
                authentication.getLookedUpBy(), version, authentication.getAuthenticationType(),
                rewriteMetadataForApiKeyRoleDescriptors(version, authentication)));
            consumer.accept(original);
        }
    }

    private Map<String, Object> rewriteMetadataForApiKeyRoleDescriptors(Version streamVersion, Authentication authentication) {
        Map<String, Object> metadata = authentication.getMetadata();
        if (authentication.getAuthenticationType() == AuthenticationType.API_KEY
            && authentication.getVersion().onOrAfter(VERSION_API_KEY_ROLES_AS_BYTES)
            && streamVersion.before(VERSION_API_KEY_ROLES_AS_BYTES)) {
            metadata = new HashMap<>(metadata);
            metadata.put(
                API_KEY_ROLE_DESCRIPTORS_KEY,
                XContentHelper.convertToMap(
                    (BytesReference) metadata.get(API_KEY_ROLE_DESCRIPTORS_KEY), false, XContentType.JSON).v2());
            metadata.put(
                API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY,
                XContentHelper.convertToMap(
                    (BytesReference) metadata.get(API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY), false, XContentType.JSON).v2());
        }
        return metadata;
    }
}
