/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authc;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Represents the result of an authentication attempt.
 * This allows a {@link Realm} to respond in 3 different ways (without needing to
 * resort to {@link org.elasticsearch.action.ActionListener#onFailure(Exception)})
 * <ol>
 * <li>Successful authentication of a user</li>
 * <li>Unable to authenticate user, try another realm (optionally with a diagnostic message)</li>
 * <li>Unable to authenticate user, terminate authentication (with an error message)</li>
 * </ol>
 */
public final class AuthenticationResult {
    private static final AuthenticationResult NOT_HANDLED = new AuthenticationResult(Status.CONTINUE, null, null, null, null);

    public static String THREAD_CONTEXT_KEY = "_xpack_security_auth_result";

    public enum Status {
        SUCCESS,
        CONTINUE,
        TERMINATE,
    }

    private final Status status;
    private final User user;
    private final String message;
    private final Exception exception;
    private final Map<String, Object> metadata;

    private AuthenticationResult(Status status, @Nullable User user, @Nullable String message, @Nullable Exception exception,
                                 @Nullable Map<String, Object> metadata) {
        this.status = status;
        this.user = user;
        this.message = message;
        this.exception = exception;
        this.metadata = metadata == null ? Collections.emptyMap() : Collections.unmodifiableMap(metadata);
    }

    public Status getStatus() {
        return status;
    }

    public User getUser() {
        return user;
    }

    public String getMessage() {
        return message;
    }

    public Exception getException() {
        return exception;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    /**
     * Creates an {@code AuthenticationResult} that indicates that the supplied {@link User}
     * has been successfully authenticated.
     * <p>
     * The {@link #getStatus() status} is set to {@link Status#SUCCESS}.
     * </p><p>
     * Neither the {@link #getMessage() message} nor {@link #getException() exception} are populated.
     * </p>
     * @param user The user that was authenticated. Cannot be {@code null}.
     */
    public static AuthenticationResult success(User user) {
        Objects.requireNonNull(user);
        return success(user, null);
    }

    /**
     * Creates a successful result, with optional metadata
     *
     * @see #success(User)
     */
    public static AuthenticationResult success(User user, @Nullable Map<String, Object> metadata) {
        return new AuthenticationResult(Status.SUCCESS, user, null, null, metadata);
    }

    /**
     * Creates an {@code AuthenticationResult} that indicates that the realm did not handle the
     * authentication request in any way, and has no failure messages.
     * <p>
     * The {@link #getStatus() status} is set to {@link Status#CONTINUE}.
     * </p><p>
     * The {@link #getMessage() message}, {@link #getException() exception}, and {@link #getUser() user} are all set to {@code null}.
     * </p>
     */
    public static AuthenticationResult notHandled() {
        return NOT_HANDLED;
    }

    /**
     * Creates an {@code AuthenticationResult} that indicates that the realm attempted to handle the authentication request but was
     * unsuccessful. The reason for the failure is given in the supplied message and optional exception.
     * <p>
     * The {@link #getStatus() status} is set to {@link Status#CONTINUE}.
     * </p><p>
     * The {@link #getUser() user} is not populated.
     * </p>
     */
    public static AuthenticationResult unsuccessful(String message, @Nullable Exception cause) {
        Objects.requireNonNull(message);
        return new AuthenticationResult(Status.CONTINUE, null, message, cause, null);
    }

    /**
     * Creates an {@code AuthenticationResult} that indicates that the realm attempted to handle the authentication request, was
     * unsuccessful and wants to terminate this authentication request.
     * The reason for the failure is given in the supplied message and optional exception.
     * <p>
     * The {@link #getStatus() status} is set to {@link Status#TERMINATE}.
     * </p><p>
     * The {@link #getUser() user} is not populated.
     * </p>
     */
    public static AuthenticationResult terminate(String message, @Nullable Exception cause) {
        return new AuthenticationResult(Status.TERMINATE, null, message, cause, null);
    }

    public boolean isAuthenticated() {
        return status == Status.SUCCESS;
    }

    @Override
    public String toString() {
        return "AuthenticationResult{" +
                "status=" + status +
                ", user=" + user +
                ", message=" + message +
                ", exception=" + exception +
                '}';
    }

}
