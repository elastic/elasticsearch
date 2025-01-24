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
public final class AuthenticationResult<T> {
    private static final AuthenticationResult<?> NOT_HANDLED = new AuthenticationResult<>(Status.CONTINUE, null, null, null, null);

    public static final String THREAD_CONTEXT_KEY = "_xpack_security_auth_result";

    public enum Status {
        /**
         * The authenticator successfully handled the authentication request
         */
        SUCCESS,
        /**
         * The authenticator either did not handle the authentication request for reasons such as
         * it cannot find necessary credentials
         * Or the authenticator tried to handle the authentication request but it was unsuccessful.
         * Subsequent authenticators (if any) still have chance to attempt authentication.
         */
        CONTINUE,
        /**
         * The authenticator fail to authenticate the request and also requires the whole authentication chain to be stopped
         */
        TERMINATE,
    }

    private final Status status;
    private final T value;
    private final String message;
    private final Exception exception;
    private final Map<String, Object> metadata;

    private AuthenticationResult(
        Status status,
        @Nullable T value,
        @Nullable String message,
        @Nullable Exception exception,
        @Nullable Map<String, Object> metadata
    ) {
        this.status = status;
        this.value = value;
        this.message = message;
        this.exception = exception;
        this.metadata = metadata == null ? Collections.emptyMap() : Collections.unmodifiableMap(metadata);
    }

    public Status getStatus() {
        return status;
    }

    public T getValue() {
        return value;
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
     * @param value The user that was authenticated. Cannot be {@code null}.
     */
    public static <T> AuthenticationResult<T> success(T value) {
        return success(value, null);
    }

    /**
     * Creates a successful result, with optional metadata
     *
     * @see #success(Object)
     */
    public static <T> AuthenticationResult<T> success(T value, @Nullable Map<String, Object> metadata) {
        Objects.requireNonNull(value);
        return new AuthenticationResult<>(Status.SUCCESS, value, null, null, metadata);
    }

    /**
     * Creates an {@code AuthenticationResult} that indicates that the realm did not handle the
     * authentication request in any way, and has no failure messages.
     * <p>
     * The {@link #getStatus() status} is set to {@link Status#CONTINUE}.
     * </p><p>
     * The {@link #getMessage() message}, {@link #getException() exception}, and {@link #getValue() user} are all set to {@code null}.
     * </p>
     */
    @SuppressWarnings("unchecked")
    public static <T> AuthenticationResult<T> notHandled() {
        return (AuthenticationResult<T>) NOT_HANDLED;
    }

    /**
     * Creates an {@code AuthenticationResult} that indicates that the realm attempted to handle the authentication request but was
     * unsuccessful. The reason for the failure is given in the supplied message and optional exception.
     * <p>
     * The {@link #getStatus() status} is set to {@link Status#CONTINUE}.
     * </p><p>
     * The {@link #getValue() value} is not populated.
     * </p>
     */
    public static <T> AuthenticationResult<T> unsuccessful(String message, @Nullable Exception cause) {
        Objects.requireNonNull(message);
        return new AuthenticationResult<>(Status.CONTINUE, null, message, cause, null);
    }

    /**
     * Creates an {@code AuthenticationResult} that indicates that the realm attempted to handle the authentication request, was
     * unsuccessful and wants to terminate this authentication request.
     * The reason for the failure is given in the supplied message and optional exception.
     * <p>
     * The {@link #getStatus() status} is set to {@link Status#TERMINATE}.
     * </p><p>
     * The {@link #getValue() value} is not populated.
     * </p>
     */
    public static <T> AuthenticationResult<T> terminate(String message, @Nullable Exception cause) {
        return new AuthenticationResult<>(Status.TERMINATE, null, message, cause, null);
    }

    /**
     * Creates an {@code AuthenticationResult} that indicates that the realm attempted to handle the authentication request, was
     * unsuccessful and wants to terminate this authentication request.
     * The reason for the failure is given in the supplied message.
     * <p>
     * The {@link #getStatus() status} is set to {@link Status#TERMINATE}.
     * </p><p>
     * The {@link #getValue() value} is not populated.
     * </p>
     */
    public static <T> AuthenticationResult<T> terminate(String message) {
        return terminate(message, null);
    }

    public boolean isAuthenticated() {
        return status == Status.SUCCESS;
    }

    @Override
    public String toString() {
        return "AuthenticationResult{"
            + "status="
            + status
            + ", value="
            + value
            + ", message="
            + message
            + ", exception="
            + exception
            + '}';
    }

}
