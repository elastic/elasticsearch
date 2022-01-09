/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.ThreadContext.StoredContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.node.Node;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.AuthenticationType;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.authc.file.FileRealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.AuthenticationContextSerializer;
import org.elasticsearch.xpack.core.security.authc.support.SecondaryAuthentication;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.elasticsearch.xpack.core.security.authc.Authentication.VERSION_API_KEY_ROLES_AS_BYTES;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.ATTACH_REALM_NAME;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.ATTACH_REALM_TYPE;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.AUTHENTICATION_KEY;

/**
 * A lightweight utility that can find the current user and authentication information for the local thread.
 */
public class SecurityContext {

    private final Logger logger = LogManager.getLogger(SecurityContext.class);

    private final ThreadContext threadContext;
    private final AuthenticationContextSerializer authenticationSerializer;
    private final Settings settings;

    public SecurityContext(Settings settings, ThreadContext threadContext) {
        this.threadContext = threadContext;
        this.authenticationSerializer = new AuthenticationContextSerializer();
        this.settings = settings;
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
     * Checks whether the user or API key of the passed in authentication can access the resources owned by the user
     * or API key of this authentication. The rules are as follows:
     *   * True if the authentications are for the same API key (same API key ID)
     *   * True if they are the same username from the same realm
     *      - For file and native realm, same realm means the same realm type
     *      - For all other realms, same realm means same realm type plus same realm name
     *   * An user and its API key cannot access each other's resources
     *   * An user and its token can access each other's resources
     *   * Two API keys are never able to access each other's resources regardless of their ownership.
     *
     *  This check is a best effort and it does not account for certain static and external changes.
     *  See also <a href="https://www.elastic.co/guide/en/elasticsearch/reference/master/security-limitations.html">
     *      security limitations</a>
     */
    public boolean canIAccessResourcesCreatedBy(@Nullable Authentication resourceCreatorAuthentication) {
        if (resourceCreatorAuthentication == null) {
            // resource creation was not authenticated (security was disabled); anyone can access such resources
            return true;
        }
        final Authentication myAuthentication = getAuthentication();
        if (myAuthentication == null) {
            // unauthenticated users cannot access any resources created by authenticated users
            return false;
        }
        if (AuthenticationType.API_KEY == myAuthentication.getAuthenticationType()
            && AuthenticationType.API_KEY == resourceCreatorAuthentication.getAuthenticationType()) {
            final boolean sameKeyId = myAuthentication.getMetadata()
                .get(AuthenticationField.API_KEY_ID_KEY)
                .equals(resourceCreatorAuthentication.getMetadata().get(AuthenticationField.API_KEY_ID_KEY));
            if (sameKeyId) {
                assert myAuthentication.getUser().principal().equals(resourceCreatorAuthentication.getUser().principal())
                    : "The same API key ID cannot be attributed to two different usernames";
            }
            return sameKeyId;
        }

        if (myAuthentication.getAuthenticationType().equals(resourceCreatorAuthentication.getAuthenticationType())
            || (AuthenticationType.REALM == myAuthentication.getAuthenticationType()
                && AuthenticationType.TOKEN == resourceCreatorAuthentication.getAuthenticationType())
            || (AuthenticationType.TOKEN == myAuthentication.getAuthenticationType()
                && AuthenticationType.REALM == resourceCreatorAuthentication.getAuthenticationType())) {
            if (false == myAuthentication.getUser().principal().equals(resourceCreatorAuthentication.getUser().principal())) {
                return false;
            }
            final Authentication.RealmRef mySourceRealm = myAuthentication.getSourceRealm();
            final Authentication.RealmRef resourceCreatorSourceRealm = resourceCreatorAuthentication.getSourceRealm();
            if (FileRealmSettings.TYPE.equals(mySourceRealm.getType()) || NativeRealmSettings.TYPE.equals(mySourceRealm.getType())) {
                return mySourceRealm.getType().equals(resourceCreatorSourceRealm.getType());
            }
            return mySourceRealm.getName().equals(resourceCreatorSourceRealm.getName())
                && mySourceRealm.getType().equals(resourceCreatorSourceRealm.getType());
        } else {
            assert EnumSet.of(
                AuthenticationType.REALM,
                AuthenticationType.API_KEY,
                AuthenticationType.TOKEN,
                AuthenticationType.ANONYMOUS,
                AuthenticationType.INTERNAL
            ).containsAll(EnumSet.of(myAuthentication.getAuthenticationType(), resourceCreatorAuthentication.getAuthenticationType()))
                : "cross AuthenticationType comparison for canAccessResourcesOf is not applicable for: "
                    + EnumSet.of(myAuthentication.getAuthenticationType(), resourceCreatorAuthentication.getAuthenticationType());
            return false;
        }
    }

    public boolean canIAccessResourcesCreatedBy(Map<String, String> resourceCreateRequestHeaders) throws IOException {
        Authentication resourceCreatorAuthentication = null;
        if (resourceCreateRequestHeaders != null && resourceCreateRequestHeaders.containsKey(AUTHENTICATION_KEY)) {
            resourceCreatorAuthentication = AuthenticationContextSerializer.decode(resourceCreateRequestHeaders.get(AUTHENTICATION_KEY));
        }
        return canIAccessResourcesCreatedBy(resourceCreatorAuthentication);
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
        final Authentication.RealmRef authenticatedBy = new Authentication.RealmRef(
            ATTACH_REALM_NAME,
            ATTACH_REALM_TYPE,
            Node.NODE_NAME_SETTING.get(settings)
        );
        final Authentication.RealmRef lookedUpBy;
        if (user.isRunAs()) {
            lookedUpBy = authenticatedBy;
        } else {
            lookedUpBy = null;
        }
        setAuthentication(
            new Authentication(user, authenticatedBy, lookedUpBy, version, AuthenticationType.INTERNAL, Collections.emptyMap())
        );
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
        // Preserve request headers other than authentication
        final Map<String, String> existingRequestHeaders = threadContext.getRequestHeadersOnly();
        final StoredContext original = threadContext.newStoredContext(true);
        final Authentication authentication = getAuthentication();
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            setAuthentication(
                new Authentication(
                    authentication.getUser(),
                    authentication.getAuthenticatedBy(),
                    authentication.getLookedUpBy(),
                    version,
                    authentication.getAuthenticationType(),
                    rewriteMetadataForApiKeyRoleDescriptors(version, authentication)
                )
            );
            existingRequestHeaders.forEach((k, v) -> {
                if (threadContext.getHeader(k) == null) {
                    threadContext.putHeader(k, v);
                }
            });
            consumer.accept(original);
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> rewriteMetadataForApiKeyRoleDescriptors(Version streamVersion, Authentication authentication) {
        Map<String, Object> metadata = authentication.getMetadata();
        if (authentication.getAuthenticationType() == AuthenticationType.API_KEY) {
            if (authentication.getVersion().onOrAfter(VERSION_API_KEY_ROLES_AS_BYTES)
                && streamVersion.before(VERSION_API_KEY_ROLES_AS_BYTES)) {
                metadata = new HashMap<>(metadata);
                metadata.put(
                    AuthenticationField.API_KEY_ROLE_DESCRIPTORS_KEY,
                    convertRoleDescriptorsBytesToMap((BytesReference) metadata.get(AuthenticationField.API_KEY_ROLE_DESCRIPTORS_KEY))
                );
                metadata.put(
                    AuthenticationField.API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY,
                    convertRoleDescriptorsBytesToMap(
                        (BytesReference) metadata.get(AuthenticationField.API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY)
                    )
                );
            } else if (authentication.getVersion().before(VERSION_API_KEY_ROLES_AS_BYTES)
                && streamVersion.onOrAfter(VERSION_API_KEY_ROLES_AS_BYTES)) {
                    metadata = new HashMap<>(metadata);
                    metadata.put(
                        AuthenticationField.API_KEY_ROLE_DESCRIPTORS_KEY,
                        convertRoleDescriptorsMapToBytes(
                            (Map<String, Object>) metadata.get(AuthenticationField.API_KEY_ROLE_DESCRIPTORS_KEY)
                        )
                    );
                    metadata.put(
                        AuthenticationField.API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY,
                        convertRoleDescriptorsMapToBytes(
                            (Map<String, Object>) metadata.get(AuthenticationField.API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY)
                        )
                    );
                }
        }
        return metadata;
    }

    private Map<String, Object> convertRoleDescriptorsBytesToMap(BytesReference roleDescriptorsBytes) {
        return XContentHelper.convertToMap(roleDescriptorsBytes, false, XContentType.JSON).v2();
    }

    private BytesReference convertRoleDescriptorsMapToBytes(Map<String, Object> roleDescriptorsMap) {
        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.map(roleDescriptorsMap);
            return BytesReference.bytes(builder);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
