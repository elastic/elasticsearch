/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cli;

import org.elasticsearch.common.Nullable;

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
