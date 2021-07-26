/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cli;

import org.elasticsearch.core.Nullable;

/**
 * An exception representing a user fixable problem in {@link Command} usage.
 */
public class UserException extends Exception {

    /** The exist status the cli should use when catching this user error. */
    public final int exitCode;

    /**
     * Constructs a UserException with an exit status and message to show the user.
     * <p>
     * To suppress cli output on error, supply a null message.
     */
    public UserException(int exitCode, @Nullable String msg) {
        super(msg);
        this.exitCode = exitCode;
    }

    /**
     * Constructs a new user exception with specified exit status, message, and underlying cause.
     * <p>
     * To suppress cli output on error, supply a null message.
     *
     * @param exitCode the exit code
     * @param msg      the message
     * @param cause    the underlying cause
     */
    public UserException(final int exitCode, @Nullable final String msg, final Throwable cause) {
        super(msg, cause);
        this.exitCode = exitCode;
    }

}
