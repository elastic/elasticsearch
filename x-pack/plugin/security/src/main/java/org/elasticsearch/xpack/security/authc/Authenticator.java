/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public interface Authenticator {

    String name();

    AuthenticationToken extractCredentials(Context context);

    /**
     * Whether authentication can proceed to authenticate as anonymous or fallback user
     * if this authenticator can extract a credentials.
     */
    default boolean canFollowedByNullTokenHandler() {
        return true;
    }

    void authenticate(Context context, ActionListener<Result> listener);

    /**
     * Gets the token from the <code>Authorization</code> header if the header begins with
     * <code>Bearer </code>
     */
    static SecureString extractBearerTokenFromHeader(ThreadContext threadContext) {
        String header = threadContext.getHeader("Authorization");
        if (Strings.hasText(header) && header.regionMatches(true,
            0,
            "Bearer ",
            0,
            "Bearer ".length()) && header.length() > "Bearer ".length()) {
            char[] chars = new char[header.length() - "Bearer ".length()];
            header.getChars("Bearer ".length(), header.length(), chars, 0);
            return new SecureString(chars);
        }
        return null;
    }

    class Context implements Closeable {
        private final ThreadContext threadContext;
        private final AuthenticationService.AuditableRequest request;
        private final User fallbackUser;
        private final boolean fallbackToAnonymous;
        private final Realms realms;
        private final List<AuthenticationToken> authenticationTokens = new ArrayList<>();
        private final List<String> unsuccessfulMessages = new ArrayList<>();
        private boolean handleNullToken = true;
        private SecureString bearerString = null;
        private List<Realm> defaultOrderedRealmList = null;
        private List<Realm> unlicensedRealms = null;

        public Context(
            ThreadContext threadContext,
            AuthenticationService.AuditableRequest request,
            User fallbackUser,
            boolean fallbackToAnonymous,
            Realms realms
        ) {
            this.threadContext = threadContext;
            this.request = request;
            this.fallbackUser = fallbackUser;
            this.fallbackToAnonymous = fallbackToAnonymous;
            this.realms = realms;
        }

        public ThreadContext getThreadContext() {
            return threadContext;
        }

        public AuthenticationService.AuditableRequest getRequest() {
            return request;
        }

        public User getFallbackUser() {
            return fallbackUser;
        }

        public boolean shouldFallbackToAnonymous() {
            return fallbackToAnonymous;
        }

        public void setHandleNullToken(boolean value) {
            handleNullToken = value;
        }

        public boolean shouldHandleNullToken() {
            return handleNullToken;
        }

        public List<String> getUnsuccessfulMessages() {
            return unsuccessfulMessages;
        }

        public void addAuthenticationToken(AuthenticationToken authenticationToken) {
            authenticationTokens.add(authenticationToken);
        }

        public AuthenticationToken getAuthenticationToken() {
            return authenticationTokens.isEmpty() ? null : authenticationTokens.get(authenticationTokens.size() - 1);
        }

        public SecureString getBearerString() {
            if (bearerString == null) {
                bearerString = extractBearerTokenFromHeader(threadContext);
            }
            return bearerString;
        }

        public List<Realm> getDefaultOrderedRealmList() {
            if (defaultOrderedRealmList == null) {
                defaultOrderedRealmList = realms.getActiveRealms();
            }
            return defaultOrderedRealmList;
        }

        public List<Realm> getUnlicensedRealms() {
            if (unlicensedRealms == null) {
                unlicensedRealms = realms.getUnlicensedRealms();
            }
            return unlicensedRealms;
        }

        public void addUnsuccessfulMessage(String message) {
            unsuccessfulMessages.add(message);
        }

        @Override
        public void close() throws IOException {
            authenticationTokens.forEach(AuthenticationToken::clearCredentials);
        }
    }

    enum Status {
        SUCCESS, UNSUCCESSFUL, NOT_HANDLED,
    }

    class Result {

        private static final Result NOT_HANDLED =
            new Result(Status.NOT_HANDLED, null, null, null);

        private final Status status;
        private final Authentication authentication;
        private final String message;
        private final Exception exception;

        public Result(
            Status status,
            @Nullable Authentication authentication,
            @Nullable String message,
            @Nullable Exception exception) {
            this.status = status;
            this.authentication = authentication;
            this.message = message;
            this.exception = exception;
        }

        public Status getStatus() {
            return status;
        }

        public Authentication getAuthentication() {
            return authentication;
        }

        public String getMessage() {
            return message;
        }

        public Exception getException() {
            return exception;
        }

        public static Result success(Authentication authentication) {
            return new Result(Status.SUCCESS, authentication, null, null);
        }

        public static Result notHandled() {
            return NOT_HANDLED;
        }

        public static Result unsuccessful(
            String message,
            @Nullable Exception cause) {
            Objects.requireNonNull(message);
            return new Result(Status.UNSUCCESSFUL, null, message, cause);
        }
    }
}
